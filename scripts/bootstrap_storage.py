from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.config.settings import get_settings


def main() -> None:
    settings = get_settings()
    settings.ensure_directories()
    print("Diretorios do lakehouse preparados com sucesso.")


if __name__ == "__main__":
    main()
