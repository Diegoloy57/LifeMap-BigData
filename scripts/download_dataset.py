
import os
import sys
import subprocess
from pathlib import Path

DATASET_SLUG = "yelp-dataset/yelp-dataset"
DATA_DIR = Path("/app/data/raw")

REQUIRED_FILES = {
    "business": "yelp_academic_dataset_business.json",
    "review": "yelp_academic_dataset_review.json",
}


def find_dataset_files():
    """Busca los archivos de Yelp en la carpeta de datos."""
    found = {}

    for name, filename in REQUIRED_FILES.items():
        matches = list(DATA_DIR.rglob(filename))
        if matches:
            found[name] = matches[0]

    return found


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    found = find_dataset_files()

    if len(found) == len(REQUIRED_FILES):
        print("Los archivos de Yelp ya están disponibles.")
    else:
        print("Descargando Yelp desde Kaggle...")

        subprocess.run(
            [
                sys.executable,
                "-m",
                "kaggle",
                "datasets",
                "download",
                "-d",
                DATASET_SLUG,
                "-p",
                str(DATA_DIR),
                "--unzip",
            ],
            check=True,
        )

        found = find_dataset_files()

    missing = set(REQUIRED_FILES) - set(found)

    if missing:
        raise FileNotFoundError(
            f"No se encontraron los archivos: {sorted(missing)}"
        )

    for name, path in found.items():
        print(f"{name}: {path} ({path.stat().st_size:,} bytes)")

    print("Dataset Yelp verificado correctamente.")


if __name__ == "__main__":
    main()
