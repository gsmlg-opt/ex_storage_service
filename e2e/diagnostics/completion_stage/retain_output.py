"""Keep Actions mask commands and masked values out of retained file bytes."""
import os
from pathlib import Path
import sys


def retain(source, output, commands):
    masks = []
    for line in source:
        if line.startswith("::add-mask::"):
            value = line[len("::add-mask::"):].rstrip("\r\n")
            if not value or len(value) > 4096 or len(masks) >= 64:
                raise ValueError("invalid diagnostic mask command")
            masks.append(value)
            # The Actions command processor registers the mask; artifact upload
            # does not redact files, so the command itself is never retained.
            commands.write(line)
            commands.flush()
        else:
            for value in masks:
                line = line.replace(value, "[REDACTED]")
            output.write(line)


def main():
    path = Path(sys.argv[1])
    if not path.is_absolute():
        raise ValueError("absolute diagnostic log required")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        retain(sys.stdin, output, sys.stdout)


if __name__ == "__main__":
    main()
