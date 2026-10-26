#include "common.h"
#include "runtime.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>

namespace rpc = stragglermr;
using namespace std::chrono_literals;

int main(int argc, char** argv) {
  try {
    if (argc != 4) throw std::runtime_error("expected endpoint, input, release-file");
    auto stub = rpc::Worker::NewStub(grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()));
    rpc::AttemptRequest request; request.set_job_id("probe"); request.set_task_id(0);
    request.set_attempt_id("a1"); request.set_input_path(argv[2]);
    auto submit = [&](const rpc::AttemptRequest& req, bool admitted, bool ok = true) {
      grpc::ClientContext ctx; ctx.set_deadline(std::chrono::system_clock::now() + 2s);
      rpc::SubmitReply reply; auto status = stub->Submit(&ctx, req, &reply);
      if (status.ok() != ok || (ok && reply.admitted() != admitted)) throw std::runtime_error("unexpected admission result");
    };
    auto inspect = [&](const std::string& job) {
      grpc::ClientContext ctx; ctx.set_deadline(std::chrono::system_clock::now() + 2s);
      rpc::InspectRequest req; req.set_job_id(job); rpc::WorkerSnapshot reply;
      if (!stub->Inspect(&ctx, req, &reply).ok()) throw std::runtime_error("Inspect failed");
      return reply;
    };
    submit(request, true); submit(request, true);
    auto conflicting = request; conflicting.set_task_id(1); submit(conflicting, false, false);
    auto second = request; second.set_attempt_id("a2"); submit(second, false);
    auto running = inspect("probe");
    if (running.active() != 1 || running.peak_active() != 1 || running.attempts_size() != 1)
      throw std::runtime_error("duplicate requests consumed capacity or capacity exceeded");
    mr::write_file(argv[3], "release\n");
    auto await_result = [&](const std::string& job) {
      const auto deadline = mr::Clock::now() + 3s;
      while (mr::Clock::now() < deadline) {
        auto snapshot = inspect(job);
        if (snapshot.active() == 0 && snapshot.attempts_size() == 1 && snapshot.attempts(0).state() == rpc::SUCCEEDED)
          return snapshot.attempts(0).artifact_path();
        std::this_thread::sleep_for(5ms);
      }
      throw std::runtime_error("attempt did not finish");
    };
    const auto first_path = await_result("probe");
    submit(request, true);
    if (inspect("probe").active() != 0) throw std::runtime_error("completed duplicate re-executed");
    second = request; second.set_job_id("probe-two"); submit(second, true);
    const auto second_path = await_result("probe-two");
    if (first_path == second_path || inspect("probe").attempts_size() != 1)
      throw std::runtime_error("job workspaces/status are not isolated");
    (void)mr::validate(mr::read_bounded(first_path, mr::max_artifact_bytes), "probe", 0, "a1", mr::max_input_bytes);
    (void)mr::validate(mr::read_bounded(second_path, mr::max_artifact_bytes), "probe-two", 0, "a1", mr::max_input_bytes);
    std::cout << "PASS duplicate admission, conflicting identity, capacity and job isolation\n";
  } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
