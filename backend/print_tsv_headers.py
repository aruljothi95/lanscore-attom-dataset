import glob
import os


def main() -> None:
    paths = sorted(glob.glob(r"d:\landscore-dataset\backend\*.tsv"))
    for p in paths:
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            header = f.readline().rstrip("\n")
        print(f"--- {os.path.basename(p)} ---")
        print(header)


if __name__ == "__main__":
    main()

