#include "common.h"
#include <iostream>
#include <stdexcept>

int main() {
  try {
    const std::string header = "STRAGGLERMR1\njob 0 a1\n";
    const std::vector<std::string> invalid{
      "", "STRAGGLERMR1\nother 0 a1\n", "STRAGGLERMR1\njob 1 a1\n", "STRAGGLERMR1\njob 0 a2\n",
      header + "a\t1", header + "a\t0\n", header + "a\t-1\n", header + "A\t1\n",
      header + "a\t1\na\t1\n", header + "b\t1\na\t1\n", header + "a\t1garbage\n",
      header + "a\t18446744073709551616\n", header + "a\t9\nb\t9\n", header + "\t1\n",
      header + "a\t1\n\n", header + std::string("a\t1\0junk\n", 9)};
    for (const auto& bytes : invalid) {
      bool rejected = false;
      try { (void)mr::validate(bytes, "job", 0, "a1", 10); }
      catch (const std::runtime_error&) { rejected = true; }
      if (!rejected) throw std::runtime_error("accepted a malformed candidate");
    }
    auto valid = mr::validate(header + "a\t2\nb\t1\n", "job", 0, "a1", 10);
    if (valid != mr::Counts{{"a", 2}, {"b", 1}} || !mr::validate(header, "job", 0, "a1", 0).empty())
      throw std::runtime_error("valid candidate rejected");
    std::cout << "PASS artifact identity, grammar, ordering and overflow rejection\n";
  } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
