from pathlib import Path


def count_rows(path: Path) -> int:
    # counts data rows (excluding header)
    with path.open("rb") as f:
        n = sum(1 for _ in f)
    return max(0, n - 1)


def main() -> None:
    base = Path(__file__).parent
    files = [
        base / "NIKASHA_ASSIGNMENTRELEASE_0001.tsv",
        base / "NIKASHA_PROPERTYDELETES_0001.tsv",
        base / "NIKASHA_PROPERTYTOBOUNDARYMATCH_PARCEL_0001.tsv",
        base / "NIKASHA_RECORDERDELETES_0001.tsv",
    ]
    for p in files:
        print(f"{p.name}: {count_rows(p)}")


if __name__ == "__main__":
    main()

