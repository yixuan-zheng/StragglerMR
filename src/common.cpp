#include "common.h"
#include <algorithm>
#include <atomic>
#include <charconv>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <ctime>
#include <openssl/evp.h>

namespace mr {
std::string sha256(const std::string& bytes) {
  unsigned char digest[EVP_MAX_MD_SIZE]; unsigned length = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), digest, &length, EVP_sha256(), nullptr) != 1 || length != 32)
    throw std::runtime_error("SHA-256 computation failed");
  constexpr char hex[] = "0123456789abcdef";
  std::string result; result.reserve(64);
  for (unsigned i = 0; i < length; ++i) { result += hex[digest[i] >> 4]; result += hex[digest[i] & 15]; }
  return result;
}
std::uint64_t thread_cpu_us() {
  timespec value{};
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0)
    throw std::runtime_error("thread CPU clock unavailable");
  return static_cast<std::uint64_t>(value.tv_sec) * 1000000 + value.tv_nsec / 1000;
}
Args::Args(int argc, char** argv) {
  bool positional = false;
  for (int i = 1; i < argc; ++i) {
    std::string key = argv[i];
    if (key == "--") { positional = true; continue; }
    if (positional) { inputs.push_back(key); continue; }
    if (!key.starts_with("--") || i + 1 == argc || values.contains(key))
      throw std::runtime_error("expected unique --option value pairs, then -- input files");
    values.emplace(key, argv[++i]);
  }
}
std::string Args::get(const std::string& key, const std::string& fallback) const {
  auto it = values.find(key); return it == values.end() ? fallback : it->second;
}
int Args::number(const std::string& key, int fallback, int low, int high) const {
  auto s = get(key, std::to_string(fallback));
  int n = 0; auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), n);
  if (ec != std::errc{} || end != s.data() + s.size() || n < low || n > high)
    throw std::runtime_error("invalid value for " + key);
  return n;
}
void Args::allow(const std::vector<std::string>& keys) const {
  for (const auto& [key, value] : values) {
    (void)value;
    if (std::find(keys.begin(), keys.end(), key) == keys.end())
      throw std::runtime_error("unknown option: " + key);
  }
}
std::string unique_id() {
  static std::atomic<unsigned> serial{0};
  std::random_device random;
  std::ostringstream out;
  out << std::hex << Clock::now().time_since_epoch().count() << random() << serial++;
  return out.str();
}
bool safe_id(const std::string& s) {
  return !s.empty() && s.size() <= 96 && s.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-") == std::string::npos;
}
std::string read_bounded(const fs::path& path, std::size_t limit) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open: " + path.string());
  std::string bytes; char buffer[8192];
  while (in.read(buffer, sizeof buffer) || in.gcount()) {
    if (bytes.size() + static_cast<std::size_t>(in.gcount()) > limit)
      throw std::runtime_error("file exceeds byte limit: " + path.string());
    bytes.append(buffer, static_cast<std::size_t>(in.gcount()));
  }
  if (!in.eof()) throw std::runtime_error("read failed: " + path.string());
  return bytes;
}
void write_file(const fs::path& path, const std::string& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) throw std::runtime_error("cannot create: " + path.string());
  out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); out.flush();
  if (!out) throw std::runtime_error("write failed: " + path.string());
  out.close();
  if (out.fail()) throw std::runtime_error("close failed: " + path.string());
}
void publish(const fs::path& path, const std::string& bytes) {
  auto stage = path; stage += ".staging-" + unique_id();
  try { write_file(stage, bytes); fs::rename(stage, path); }
  catch (...) { std::error_code ec; fs::remove(stage, ec); throw; }
}
Counts count_words(const std::string& bytes) {
  Counts counts; std::string word;
  for (unsigned char c : bytes) {
    if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    if (c >= 'a' && c <= 'z') word += static_cast<char>(c);
    else if (!word.empty()) { ++counts[word]; word.clear(); }
  }
  if (!word.empty()) ++counts[word];
  return counts;
}
void add_counts(Counts& into, const Counts& from) {
  for (auto& [word, count] : from) {
    if (std::numeric_limits<std::uint64_t>::max() - into[word] < count)
      throw std::runtime_error("count overflow");
    into[word] += count;
  }
}
std::string format_counts(const Counts& counts) {
  std::ostringstream out;
  for (auto& [word, count] : counts) out << word << '\t' << count << '\n';
  return out.str();
}
std::string artifact(const std::string& job, unsigned task, const std::string& attempt,
                     const Counts& counts) {
  return "STRAGGLERMR1\n" + job + " " + std::to_string(task) + " " + attempt + "\n" + format_counts(counts);
}
Counts validate(const std::string& bytes, const std::string& job, unsigned task,
                const std::string& attempt, std::size_t input_bytes) {
  const auto header = artifact(job, task, attempt, {});
  if (!bytes.starts_with(header)) throw std::runtime_error("artifact identity/header mismatch");
  Counts counts; std::string previous; std::uint64_t total = 0;
  std::size_t pos = header.size();
  while (pos < bytes.size()) {
    auto end = bytes.find('\n', pos); auto tab = bytes.find('\t', pos);
    if (end == std::string::npos || tab == std::string::npos || tab >= end)
      throw std::runtime_error("malformed artifact row");
    auto word = bytes.substr(pos, tab - pos);
    if (word.empty() || word.find_first_not_of("abcdefghijklmnopqrstuvwxyz") != std::string::npos || word <= previous)
      throw std::runtime_error("artifact keys must be sorted unique ASCII words");
    std::uint64_t count = 0;
    auto [p, ec] = std::from_chars(bytes.data() + tab + 1, bytes.data() + end, count);
    if (ec != std::errc{} || p != bytes.data() + end || count == 0 || count > input_bytes || total > input_bytes - count)
      throw std::runtime_error("invalid artifact count");
    counts.emplace(word, count); previous = word; total += count; pos = end + 1;
  }
  return counts;
}
} // namespace mr
