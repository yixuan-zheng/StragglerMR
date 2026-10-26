"""Independent serial specification: ASCII letter runs, case folded, sorted TSV."""
import collections
import pathlib
import re


def serial_result(paths):
    counts = collections.Counter()
    for path in paths:
        counts.update(word.lower() for word in re.findall(rb"[A-Za-z]+", pathlib.Path(path).read_bytes()))
    return b"".join(word + b"\t" + str(count).encode("ascii") + b"\n"
                    for word, count in sorted(counts.items()))


if __name__ == "__main__":
    import sys
    sys.stdout.buffer.write(serial_result(sys.argv[1:]))
