#include "common.h"
#include "runtime.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <set>
#include <thread>

namespace rpc = stragglermr;
using namespace std::chrono_literals;

struct Attempt {
  unsigned task;
  std::size_t worker;
  mr::Clock::time_point started;
  bool retired = false, observed_done = false;
  rpc::AttemptReason reason = rpc::NORMAL;
  bool winner = false;
  std::uint64_t runtime_us = 0, cpu_us = 0;
};
struct Task {
  mr::fs::path input;
  std::size_t bytes;
  int launches = 0;
  bool accepted = false;
  bool backup_launched = false;
  rpc::AttemptReason retry_reason = rpc::FAILURE_RETRY;
};
struct Remote {
  std::unique_ptr<rpc::Worker::Stub> stub;
  std::string instance;
  bool healthy = true;
  unsigned capacity = 0, active = 0, peak = 0;
};

class Coordinator {
 public:
  explicit Coordinator(const mr::Args& args)
      : run_(mr::fs::absolute(args.get("--run-dir"))), job_(mr::unique_id()),
        max_attempts_(args.number("--max-attempts", 3, 1, 5)),
        timeout_ms_(args.number("--attempt-timeout-ms", 2000, 50, 60000)),
        rpc_ms_(args.number("--rpc-timeout-ms", 250, 20, 5000)),
        job_ms_(args.number("--job-timeout-ms", 30000, 100, 300000)),
        drain_ms_(args.number("--drain-ms", 0, 0, 5000)),
        speculation_(args.get("--speculation", "off") == "on") {
    if (args.get("--speculation", "off") != "on" && args.get("--speculation", "off") != "off")
      throw std::runtime_error("--speculation must be on or off");
    if (args.get("--run-dir").empty() || args.inputs.empty() || args.inputs.size() > 1000)
      throw std::runtime_error("usage: mr_coordinator --run-dir NEW_DIRECTORY --workers host:port,host:port -- input files (1..1000)");
    std::string endpoints = args.get("--workers"); std::size_t start = 0;
    std::vector<std::string> seen;
    while (start < endpoints.size()) {
      auto end = endpoints.find(',', start);
      auto address = endpoints.substr(start, end == std::string::npos ? end : end - start);
      if (!address.starts_with("127.0.0.1:") || std::find(seen.begin(), seen.end(), address) != seen.end())
        throw std::runtime_error("workers must be distinct loopback endpoints");
      seen.push_back(address);
      auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
      remotes_.push_back({rpc::Worker::NewStub(channel), "", true, 0, 0, 0});
      if (end == std::string::npos) break;
      start = end + 1;
    }
    if (remotes_.empty() || remotes_.size() > 16) throw std::runtime_error("expected 1..16 workers");
    output_ = args.get("--output", (run_ / "result.tsv").string());
    if (mr::fs::exists(output_)) throw std::runtime_error("output must not already exist");
    if (!mr::fs::create_directory(run_)) throw std::runtime_error("run directory must be new");
    mr::fs::create_directory(run_ / "inputs"); mr::fs::create_directory(run_ / "accepted");
    events_.open(run_ / "events.tsv");
    if (!events_) throw std::runtime_error("cannot open event log");
    std::size_t total = 0;
    for (auto& input : args.inputs) {
      const auto bytes = mr::read_bounded(input, mr::max_input_bytes);
      total += bytes.size();
      if (total > 64 * 1024 * 1024) throw std::runtime_error("job input exceeds 64 MiB limit");
      const auto path = run_ / "inputs" / (std::to_string(tasks_.size()) + ".txt");
      mr::write_file(path, bytes); tasks_.push_back({path, bytes.size(), 0, false});
    }
    event("job_started", 0, "-", 0, job_);
  }
  int run() {
    try {
      while (accepted_ != tasks_.size()) {
        if (elapsed_ms() >= job_ms_) throw std::runtime_error("job deadline exceeded");
        inspect_all(); expire(); dispatch(); speculate();
        if (accepted_ != tasks_.size()) std::this_thread::sleep_for(10ms);
      }
      const auto map_done = elapsed_ms() - map_started_ms_;
      // Merge only coordinator-owned validated snapshots, never worker paths.
      mr::publish(output_, mr::format_counts(merged_));
      committed_ms_ = elapsed_ms();
      event("job_committed", 0, "-", 0, output_.string());
      const auto until = mr::Clock::now() + std::chrono::milliseconds(drain_ms_);
      while (mr::Clock::now() < until && has_unfinished()) { inspect_all(); std::this_thread::sleep_for(10ms); }
      write_metrics(true, map_done); return 0;
    } catch (const std::exception& error) {
      event("job_failed", 0, "-", 0, error.what()); write_metrics(false, 0);
      std::cerr << "job failed: " << error.what() << '\n'; return 1;
    }
  }
 private:
  long long elapsed_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(mr::Clock::now() - start_).count();
  }
  void event(const std::string& kind, unsigned task, const std::string& attempt,
             std::size_t worker, std::string detail = "") {
    std::replace(detail.begin(), detail.end(), '\n', ' ');
    std::replace(detail.begin(), detail.end(), '\t', ' ');
    events_ << elapsed_ms() << '\t' << kind << '\t' << task << '\t' << attempt << '\t' << worker << '\t' << detail << '\n';
    events_.flush();
    if (!events_) throw std::runtime_error("event log write failed");
  }
  void deadline(grpc::ClientContext& ctx) const {
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(rpc_ms_));
  }
  bool task_running(unsigned task) const {
    return std::any_of(attempts_.begin(), attempts_.end(), [&](auto& entry) {
      return entry.second.task == task && !entry.second.retired;
    });
  }
  bool has_unfinished() const {
    return std::any_of(attempts_.begin(), attempts_.end(), [&](auto& entry) {
      return !entry.second.observed_done && remotes_[entry.second.worker].healthy;
    });
  }
  static std::string reason_name(rpc::AttemptReason reason) {
    switch (reason) {
      case rpc::NORMAL: return "normal";
      case rpc::FAILURE_RETRY: return "failure_retry";
      case rpc::DEADLINE_RETRY: return "deadline_retry";
      case rpc::SPECULATION: return "speculation";
      default: throw std::runtime_error("invalid attempt reason");
    }
  }
  std::size_t speculative_outstanding() const {
    return std::count_if(attempts_.begin(), attempts_.end(), [](const auto& entry) {
      return entry.second.reason == rpc::SPECULATION && !entry.second.observed_done;
    });
  }
  void inspect_all() {
    for (std::size_t w = 0; w < remotes_.size(); ++w) {
      auto& remote = remotes_[w]; if (!remote.healthy) continue;
      grpc::ClientContext ctx; deadline(ctx);
      rpc::InspectRequest request; request.set_job_id(job_); rpc::WorkerSnapshot snapshot;
      auto status = remote.stub->Inspect(&ctx, request, &snapshot);
      if (!status.ok() || (!remote.instance.empty() && snapshot.instance_id() != remote.instance)) {
        // No restart/rejoin protocol: quarantine the endpoint for this job.
        remote.healthy = false; event("worker_unavailable", 0, "-", w, status.error_message());
        for (auto& [id, attempt] : attempts_) {
          if (attempt.worker == w && !attempt.retired) {
            attempt.retired = true; ++failed_; event("attempt_failed", attempt.task, id, w, "worker unavailable");
            tasks_[attempt.task].retry_reason = rpc::FAILURE_RETRY;
          }
        }
        continue;
      }
      if (snapshot.capacity() == 0 || snapshot.capacity() > 16 || snapshot.active() > snapshot.capacity() ||
          snapshot.peak_active() > snapshot.capacity()) throw std::runtime_error("invalid worker capacity snapshot");
      remote.instance = snapshot.instance_id(); remote.capacity = snapshot.capacity();
      remote.active = snapshot.active(); remote.peak = snapshot.peak_active();
      for (const auto& record : snapshot.attempts()) {
        auto found = attempts_.find(record.request().attempt_id());
        if (found == attempts_.end() || found->second.worker != w || record.request().job_id() != job_ ||
            record.request().task_id() != found->second.task) throw std::runtime_error("unexpected attempt identity");
        auto& attempt = found->second;
        if (record.state() == rpc::RUNNING || attempt.observed_done) continue;
        attempt.observed_done = true;
        attempt.runtime_us = record.runtime_us(); attempt.cpu_us = record.cpu_us();
        auto& task = tasks_.at(attempt.task);
        if (attempt.retired || task.accepted) {
          attempt.retired = true; ++rejected_; event("late_rejected", attempt.task, found->first, w); continue;
        }
        attempt.retired = true;
        if (record.state() != rpc::SUCCEEDED) {
          task.retry_reason = rpc::FAILURE_RETRY;
          ++failed_; event("attempt_failed", attempt.task, found->first, w, record.error()); continue;
        }
        mr::Counts counts;
        try {
          const mr::fs::path candidate(record.artifact_path());
          if (!candidate.is_absolute() || !mr::fs::is_regular_file(mr::fs::symlink_status(candidate)))
            throw std::runtime_error("candidate is not a regular file");
          // Open once, validate the exact bytes read, then regenerate under coordinator ownership.
          const auto bytes = mr::read_bounded(candidate, mr::max_artifact_bytes);
          if (record.artifact_bytes() != bytes.size() || record.artifact_sha256() != mr::sha256(bytes))
            throw std::runtime_error("candidate checksum/size mismatch");
          counts = mr::validate(bytes, job_, attempt.task, found->first, task.bytes);
        } catch (const std::exception& error) {
          task.retry_reason = rpc::FAILURE_RETRY;
          ++failed_; ++invalid_; event("invalid_candidate", attempt.task, found->first, w, error.what()); continue;
        }
        mr::publish(run_ / "accepted" / (std::to_string(attempt.task) + ".txt"),
                    mr::artifact(job_, attempt.task, found->first, counts));
        mr::add_counts(merged_, counts); task.accepted = true; ++accepted_;
        attempt.winner = true;
        if (attempt.reason == rpc::SPECULATION) ++backup_wins_;
        if (attempt.reason == rpc::NORMAL)
          references_.push_back({task.bytes, std::max<std::uint64_t>(1, record.runtime_us())});
        for (auto& [other_id, other] : attempts_) {
          if (other.task == attempt.task && !other.retired) {
            other.retired = true; event("superseded", other.task, other_id, other.worker);
          }
        }
        event("accepted", attempt.task, found->first, w);
      }
    }
  }
  void expire() {
    for (auto& [id, attempt] : attempts_) {
      if (!attempt.retired && mr::Clock::now() - attempt.started >= std::chrono::milliseconds(timeout_ms_)) {
        attempt.retired = true; ++expired_; event("expired", attempt.task, id, attempt.worker);
        tasks_[attempt.task].retry_reason = rpc::DEADLINE_RETRY;
        // Do not release the worker's slot: Inspect remains authoritative about active execution.
      }
    }
  }
  void launch(unsigned t, std::size_t w, rpc::AttemptReason reason) {
    auto& task = tasks_[t]; auto& remote = remotes_[w];
    if (map_started_ms_ < 0) { map_started_ms_ = elapsed_ms(); event("map_started", t, "-", w); }
    const auto id = "t" + std::to_string(t) + "-a" + std::to_string(++task.launches);
    Attempt attempt{t, w, mr::Clock::now(), false, false}; attempt.reason = reason;
    attempts_.emplace(id, attempt);
    rpc::AttemptRequest request; request.set_job_id(job_); request.set_task_id(t);
    request.set_attempt_id(id); request.set_input_path(task.input.string()); request.set_reason(reason);
    grpc::ClientContext ctx; deadline(ctx); rpc::SubmitReply reply;
    ++remote.active; ++requests_; event("submitted", t, id, w, reason_name(reason));
    speculative_peak_ = std::max(speculative_peak_, speculative_outstanding());
    auto status = remote.stub->Submit(&ctx, request, &reply);
    if (!status.ok()) { ++uncertain_; event("submit_uncertain", t, id, w, status.error_message()); }
    else if (!reply.admitted()) {
      attempts_.at(id).retired = true; attempts_.at(id).observed_done = true;
      task.retry_reason = rpc::FAILURE_RETRY;
      ++failed_; ++admission_rejected_; event("admission_rejected", t, id, w, reply.reason());
    } else ++admitted_replies_;
  }
  void dispatch() {
    for (unsigned t = 0; t < tasks_.size(); ++t) {
      auto& task = tasks_[t]; if (task.accepted || task_running(t)) continue;
      if (task.launches >= max_attempts_) throw std::runtime_error("attempt budget exhausted for task " + std::to_string(t));
      bool healthy = false;
      for (std::size_t w = 0; w < remotes_.size(); ++w) {
        auto& remote = remotes_[w]; if (!remote.healthy) continue;
        healthy = true; if (remote.active >= remote.capacity) continue;
        launch(t, w, task.launches == 0 ? rpc::NORMAL : task.retry_reason);
        break;
      }
      if (!healthy) throw std::runtime_error("no reachable workers remain");
    }
  }
  void speculate() {
    if (!speculation_) return;
    auto blocked = [&](unsigned task, const std::string& reason) {
      if (blocked_.insert(std::to_string(task) + reason + std::to_string(accepted_)).second)
        event("speculation_blocked", task, "-", 0, reason);
    };
    for (unsigned t = 0; t < tasks_.size(); ++t)
      if (!tasks_[t].accepted && !task_running(t)) { blocked(t, "ordinary_pending"); return; }
    if (speculative_outstanding() >= 2) { blocked(0, "concurrent_limit"); return; }
    for (unsigned t = 0; t < tasks_.size(); ++t) {
      auto& task = tasks_[t];
      if (task.accepted || task.backup_launched || task.launches != 1) continue;
      auto original = std::find_if(attempts_.begin(), attempts_.end(), [&](const auto& entry) {
        return entry.second.task == t && !entry.second.retired && entry.second.reason == rpc::NORMAL;
      });
      if (original == attempts_.end()) continue;
      std::vector<std::uint64_t> samples;
      for (auto [size, runtime] : references_) {
        if (std::min(size, task.bytes) * 5 >= std::max(size, task.bytes) * 4) samples.push_back(runtime);
      }
      if (samples.size() < 5) { blocked(t, "insufficient_reference"); continue; }
      std::sort(samples.begin(), samples.end());
      const auto median = samples[samples.size() / 2];
      const auto threshold = std::max<std::uint64_t>(200000, 3 * median);
      const auto age = std::chrono::duration_cast<std::chrono::microseconds>(mr::Clock::now() - original->second.started).count();
      if (age < static_cast<long long>(threshold) || age + rpc_ms_ * 1000LL >= timeout_ms_ * 1000LL) continue;
      if (task.launches >= max_attempts_) { blocked(t, "attempt_budget"); continue; }
      bool launched = false;
      for (std::size_t w = 0; w < remotes_.size(); ++w) {
        auto& remote = remotes_[w];
        if (!remote.healthy || remote.active >= remote.capacity || original->second.worker == w) continue;
        const auto launch_age = std::chrono::duration_cast<std::chrono::microseconds>(mr::Clock::now() - original->second.started).count();
        if (launch_age + rpc_ms_ * 1000LL >= timeout_ms_ * 1000LL) break;
        task.backup_launched = true;
        event("speculation_decision", t, original->first, w, "peers=" + std::to_string(samples.size()) +
          " median_us=" + std::to_string(median) + " threshold_us=" + std::to_string(threshold) +
          " original_age_us=" + std::to_string(launch_age));
        launch(t, w, rpc::SPECULATION); launched = true; break;
      }
      if (!launched) blocked(t, "no_idle_other_worker");
      if (speculative_outstanding() >= 2) break;
    }
  }
  void write_metrics(bool success, long long map_ms) {
    std::uint64_t slot = 0, cpu = 0, wasted_slot = 0, wasted_cpu = 0, spec_slot = 0, spec_cpu = 0;
    std::size_t specs = 0, unresolved = 0;
    for (const auto& [id, attempt] : attempts_) {
      (void)id;
      slot += attempt.runtime_us; cpu += attempt.cpu_us;
      if (!attempt.winner) { wasted_slot += attempt.runtime_us; wasted_cpu += attempt.cpu_us; }
      if (attempt.reason == rpc::SPECULATION) { ++specs; spec_slot += attempt.runtime_us; spec_cpu += attempt.cpu_us; }
      if (!attempt.observed_done) ++unresolved;
    }
    std::ostringstream out;
    out << std::boolalpha << "{\n  \"job_id\": \"" << job_ << "\",\n  \"success\": " << success
        << ",\n  \"speculation_enabled\": " << speculation_ << ",\n  \"tasks\": " << tasks_.size()
        << ",\n  \"accepted\": " << accepted_ << ",\n  \"attempts\": " << attempts_.size()
        << ",\n  \"expired\": " << expired_ << ",\n  \"failed_attempts\": " << failed_
        << ",\n  \"invalid_candidates\": " << invalid_ << ",\n  \"late_rejected\": " << rejected_
        << ",\n  \"uncertain_submissions\": " << uncertain_ << ",\n  \"submit_requests\": " << requests_
        << ",\n  \"admitted_replies\": " << admitted_replies_ << ",\n  \"admission_rejections\": " << admission_rejected_
        << ",\n  \"speculative_attempts\": " << specs << ",\n  \"backup_wins\": " << backup_wins_
        << ",\n  \"speculative_outstanding_peak\": " << speculative_peak_
        << ",\n  \"unresolved_attempts\": " << unresolved
        << ",\n  \"observed_slot_us\": " << slot << ",\n  \"observed_cpu_us\": " << cpu
        << ",\n  \"observed_wasted_slot_us\": " << wasted_slot << ",\n  \"observed_wasted_cpu_us\": " << wasted_cpu
        << ",\n  \"observed_speculative_slot_us\": " << spec_slot << ",\n  \"observed_speculative_cpu_us\": " << spec_cpu
        << ",\n  \"map_ms\": " << map_ms << ",\n  \"job_ms\": " << committed_ms_
        << ",\n  \"elapsed_ms\": " << elapsed_ms() << ",\n  \"workers\": [";
    for (std::size_t w = 0; w < remotes_.size(); ++w) {
      if (w) out << ", ";
      out << "{\"capacity\":" << remotes_[w].capacity << ",\"peak_active\":" << remotes_[w].peak << "}";
    }
    out << "],\n  \"attempt_records\": [";
    bool first = true;
    for (const auto& [id, a] : attempts_) {
      if (!first) out << ", ";
      first = false;
      out << "{\"id\":\"" << id << "\",\"task\":" << a.task << ",\"worker\":" << a.worker
          << ",\"reason\":\"" << reason_name(a.reason) << "\",\"winner\":" << a.winner
          << ",\"terminal_observed\":" << a.observed_done << ",\"retired\":" << a.retired
          << ",\"runtime_us\":" << a.runtime_us << ",\"cpu_us\":" << a.cpu_us << "}";
    }
    out << "]\n}\n"; mr::publish(run_ / "metrics.json", out.str());
  }
  mr::fs::path run_, output_;
  std::string job_;
  int max_attempts_, timeout_ms_, rpc_ms_, job_ms_, drain_ms_;
  mr::Clock::time_point start_ = mr::Clock::now();
  bool speculation_;
  long long committed_ms_ = 0, map_started_ms_ = -1;
  std::vector<std::pair<std::size_t, std::uint64_t>> references_;
  std::set<std::string> blocked_;
  std::size_t backup_wins_ = 0, speculative_peak_ = 0, requests_ = 0, admitted_replies_ = 0, admission_rejected_ = 0;
  std::ofstream events_;
  std::vector<Task> tasks_;
  std::vector<Remote> remotes_;
  std::map<std::string, Attempt> attempts_;
  mr::Counts merged_;
  std::size_t accepted_ = 0, failed_ = 0, invalid_ = 0, expired_ = 0, rejected_ = 0, uncertain_ = 0;
};

int main(int argc, char** argv) {
  try {
    mr::Args args(argc, argv);
    args.allow({"--run-dir", "--workers", "--max-attempts", "--attempt-timeout-ms", "--rpc-timeout-ms",
                "--job-timeout-ms", "--drain-ms", "--output", "--speculation"});
    Coordinator coordinator(args); return coordinator.run();
  } catch (const std::exception& error) {
    std::cerr << "coordinator failed: " << error.what() << '\n'; return 1;
  }
}
