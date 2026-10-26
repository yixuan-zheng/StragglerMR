#include "common.h"
#include "runtime.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <mutex>
#include <thread>

namespace rpc = stragglermr;
using namespace std::chrono_literals;

class Worker final : public rpc::Worker::Service {
 public:
  Worker(mr::fs::path root, int capacity, const mr::Args& args)
      : root_(std::move(root)), capacity_(capacity), instance_(mr::unique_id()),
        fault_(args.get("--fault", "none")), fault_task_(args.number("--fault-task", 0, 0, 999)),
        delay_ms_(args.number("--delay-ms", 0, 0, 60000)),
        service_ms_(args.number("--service-ms", 0, 0, 5000)),
        lose_reply_(args.number("--lose-reply", 0, 0, 1)),
        fault_reason_(args.get("--fault-reason", "any")),
        release_(args.get("--release-file")), event_(args.get("--event-file")) {
    const std::vector<std::string> faults{"none", "error", "malformed", "missing", "crash", "hold", "delay", "lost-reply",
      "hold-error", "hold-crash", "hold-malformed", "hold-corrupt", "delay-crash"};
    if (std::find(faults.begin(), faults.end(), fault_) == faults.end())
      throw std::runtime_error("unknown fault kind");
    if (fault_.starts_with("hold") && release_.empty()) throw std::runtime_error("hold needs --release-file");
    if (fault_reason_ != "any" && fault_reason_ != "normal" && fault_reason_ != "speculation")
      throw std::runtime_error("invalid --fault-reason");
    if (!mr::fs::create_directory(root_)) throw std::runtime_error("worker root must be a new directory");
    try {
      for (int i = 0; i < capacity_; ++i) threads_.emplace_back([this] { execute_loop(); });
    } catch (...) {
      { std::lock_guard lock(mutex_); stopping_ = true; }
      cv_.notify_all();
      for (auto& thread : threads_) thread.join();
      throw;
    }
  }
  ~Worker() override {
    { std::lock_guard lock(mutex_); stopping_ = true; }
    cv_.notify_all();
    for (auto& thread : threads_) thread.join();
  }
  grpc::Status Submit(grpc::ServerContext*, const rpc::AttemptRequest* request, rpc::SubmitReply* reply) override {
    if (!mr::safe_id(request->job_id()) || !mr::safe_id(request->attempt_id()) ||
        request->input_path().size() > 4096 || !mr::fs::path(request->input_path()).is_absolute())
      return {grpc::StatusCode::INVALID_ARGUMENT, "invalid identity/input path"};
    std::lock_guard lock(mutex_);
    auto key = request->job_id() + "/" + request->attempt_id();
    auto existing = records_.find(key);
    if (existing != records_.end()) {
      if (existing->second.request().SerializeAsString() != request->SerializeAsString())
        return {grpc::StatusCode::ALREADY_EXISTS, "attempt identity reused with different request"};
      reply->set_admitted(true); return grpc::Status::OK;
    }
    if (active_ >= capacity_ || records_.size() >= 10000) {
      reply->set_reason("worker capacity/history limit"); return grpc::Status::OK;
    }
    rpc::AttemptRecord record; *record.mutable_request() = *request;
    record.set_state(rpc::RUNNING); records_.emplace(key, record);
    ++active_; peak_ = std::max(peak_, active_);
    bool fault = !fault_used_ && request->task_id() == static_cast<unsigned>(fault_task_) &&
      (fault_reason_ == "any" || (fault_reason_ == "normal" && request->reason() == rpc::NORMAL) ||
       (fault_reason_ == "speculation" && request->reason() == rpc::SPECULATION));
    if (fault) fault_used_ = true;
    queue_.push_back({key, fault}); cv_.notify_one(); reply->set_admitted(true);
    if (fault && (fault_ == "lost-reply" || lose_reply_))
      return {grpc::StatusCode::UNAVAILABLE, "injected response loss after admission"};
    return grpc::Status::OK;
  }
  grpc::Status Inspect(grpc::ServerContext*, const rpc::InspectRequest* request, rpc::WorkerSnapshot* reply) override {
    std::lock_guard lock(mutex_);
    reply->set_instance_id(instance_); reply->set_capacity(capacity_);
    reply->set_active(active_); reply->set_peak_active(peak_);
    for (auto& [key, record] : records_) {
      (void)key;
      if (record.request().job_id() == request->job_id()) *reply->add_attempts() = record;
    }
    return grpc::Status::OK;
  }
 private:
  void execute_loop() {
    while (true) {
      std::string key; bool fault; rpc::AttemptRecord result;
      {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
        if (stopping_ && queue_.empty()) return;
        std::tie(key, fault) = queue_.front(); queue_.pop_front(); result = records_.at(key);
      }
      const auto start = mr::Clock::now();
      const auto cpu_start = mr::thread_cpu_us();
      const auto& req = result.request();
      const auto dir = root_ / req.job_id() / std::to_string(req.task_id()) / req.attempt_id();
      auto timing = [&](const std::string& state) {
        result.set_runtime_us(std::chrono::duration_cast<std::chrono::microseconds>(mr::Clock::now() - start).count());
        result.set_cpu_us(mr::thread_cpu_us() - cpu_start);
        mr::publish(dir / "timing.tsv", req.job_id() + "\t" + std::to_string(req.task_id()) + "\t" + req.attempt_id() +
          "\t" + std::to_string(req.reason()) + "\t" + state + "\t" + std::to_string(result.runtime_us()) +
          "\t" + std::to_string(result.cpu_us()) + "\n");
      };
      try {
        mr::fs::create_directories(dir);
        const auto path = dir / "candidate.txt";
        if (service_ms_) std::this_thread::sleep_for(std::chrono::milliseconds(service_ms_));
        if (fault && !event_.empty()) mr::publish(event_, req.attempt_id() + "\n");
        if (fault && fault_ == "crash") {
          mr::write_file(path, "partial"); timing("crashed"); std::_Exit(86);
        }
        if (fault && fault_.starts_with("hold")) {
          // File acknowledgment makes the late-attempt test independent of scheduling sleeps.
          const auto end = mr::Clock::now() + 15s;
          while (!mr::fs::exists(release_)) {
            if (mr::Clock::now() >= end) throw std::runtime_error("hold release deadline exceeded");
            std::this_thread::sleep_for(5ms);
          }
        }
        if (fault && (fault_ == "delay" || fault_ == "delay-crash"))
          std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
        if (fault && (fault_ == "hold-crash" || fault_ == "delay-crash")) { timing("crashed"); std::_Exit(86); }
        if (fault && (fault_ == "error" || fault_ == "hold-error")) throw std::runtime_error("injected worker error");
        const auto counts = mr::count_words(mr::read_bounded(req.input_path(), mr::max_input_bytes));
        if (!(fault && fault_ == "missing")) {
          auto bytes = fault && (fault_ == "malformed" || fault_ == "hold-malformed") ? "invalid candidate\n" :
            mr::artifact(req.job_id(), req.task_id(), req.attempt_id(), counts);
          result.set_artifact_sha256(mr::sha256(bytes)); result.set_artifact_bytes(bytes.size());
          if (fault && fault_ == "hold-corrupt" && !counts.empty()) {
            // Preserve valid framing/identity and a plausible positive count, but corrupt the attested payload.
            auto& digit = bytes.at(bytes.size() - 2); digit = digit == '9' ? '8' : digit + 1;
          }
          mr::publish(path, bytes);
        }
        result.set_artifact_path(path.string()); result.set_state(rpc::SUCCEEDED);
      } catch (const std::exception& error) {
        result.set_state(rpc::FAILED); result.set_error(error.what());
      }
      try { timing(result.state() == rpc::SUCCEEDED ? "succeeded" : "failed"); }
      catch (const std::exception& error) { result.set_state(rpc::FAILED); result.set_error(error.what()); }
      {
        std::lock_guard lock(mutex_); records_.at(key) = std::move(result); --active_;
      }
    }
  }
  mr::fs::path root_;
  int capacity_;
  std::string instance_, fault_;
  int fault_task_, delay_ms_, service_ms_, lose_reply_;
  std::string fault_reason_;
  mr::fs::path release_, event_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stopping_ = false, fault_used_ = false;
  int active_ = 0, peak_ = 0;
  std::map<std::string, rpc::AttemptRecord> records_;
  std::deque<std::pair<std::string, bool>> queue_;
  std::vector<std::thread> threads_;
};

int main(int argc, char** argv) {
  try {
    mr::Args args(argc, argv);
    args.allow({"--root", "--listen", "--ready-file", "--capacity", "--fault", "--fault-task",
                "--delay-ms", "--release-file", "--event-file", "--service-ms", "--lose-reply", "--fault-reason"});
    if (args.get("--root").empty() || !args.inputs.empty()) throw std::runtime_error("usage: mr_worker --root NEW_DIRECTORY [--listen 127.0.0.1:50051]");
    const auto listen = args.get("--listen", "127.0.0.1:50051");
    if (!listen.starts_with("127.0.0.1:")) throw std::runtime_error("only loopback endpoints are supported");
    Worker worker(mr::fs::absolute(args.get("--root")), args.number("--capacity", 1, 1, 16), args);
    grpc::ServerBuilder builder; int port = 0;
    builder.AddListeningPort(listen, grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&worker);
    builder.SetMaxReceiveMessageSize(1024 * 1024);
    auto server = builder.BuildAndStart();
    if (!server || port == 0) throw std::runtime_error("cannot listen on " + listen);
    const auto address = "127.0.0.1:" + std::to_string(port);
    if (!args.get("--ready-file").empty()) mr::publish(args.get("--ready-file"), address + "\n");
    std::cout << "READY " << address << std::endl;
    server->Wait();
  } catch (const std::exception& error) {
    std::cerr << "worker failed: " << error.what() << '\n'; return 1;
  }
}
