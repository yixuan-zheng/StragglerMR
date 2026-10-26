#pragma once
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace mr {
namespace fs = std::filesystem;
using Counts = std::map<std::string, std::uint64_t>;
using Clock = std::chrono::steady_clock;
constexpr std::size_t max_input_bytes = 4 * 1024 * 1024;
constexpr std::size_t max_artifact_bytes = 32 * 1024 * 1024;
struct Args {
  std::map<std::string, std::string> values;
  std::vector<std::string> inputs;
  Args(int argc, char** argv);
  std::string get(const std::string& key, const std::string& fallback = "") const;
  int number(const std::string& key, int fallback, int low, int high) const;
  void allow(const std::vector<std::string>& keys) const;
};
std::string unique_id();
std::uint64_t thread_cpu_us();
std::string sha256(const std::string& bytes);
bool safe_id(const std::string& value);
std::string read_bounded(const fs::path& path, std::size_t limit);
void write_file(const fs::path& path, const std::string& bytes);
void publish(const fs::path& path, const std::string& bytes);
Counts count_words(const std::string& bytes);
void add_counts(Counts& into, const Counts& from);
std::string format_counts(const Counts& counts);
std::string artifact(const std::string& job, unsigned task, const std::string& attempt,
                     const Counts& counts);
Counts validate(const std::string& bytes, const std::string& job, unsigned task,
                const std::string& attempt, std::size_t input_bytes);
} // namespace mr
