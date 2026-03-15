import sys


def info(msg: str):
    print(f"[INFO] {msg}")


def warn(msg: str):
    print(f"[WARN] {msg}")


def error(msg: str):
    print(f"[ERROR] {msg}")


def progress(current: int, total: int, msg: str = ""):
    line = f"[{current}/{total}] {msg}" if msg else f"[{current}/{total}]"
    sys.stdout.write("\r\033[K" + line)
    sys.stdout.flush()

    if current == total:
        print()


def done(msg: str = "Done."):
    print(msg)
