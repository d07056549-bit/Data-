import yaml
from pathlib import Path

from .logging_conf import setup_logging
from .io_utils import list_data_files
from .harmonize import harmonize_single_file, save_harmonized

def load_yaml(path: Path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def main():
    logger = setup_logging()

    settings = load_yaml(Path("config/settings.yaml"))

    raw_root = Path(settings["raw_root"])
    processed_root = Path(settings["processed_root"])

    files = list_data_files(raw_root)
    logger.info(f"Found {len(files)} raw files under {raw_root}")

    for f in files:
        logger.info(f"Processing {f}")
        result = harmonize_single_file(f, settings, logger)
        if result is None:
            continue

        rel_path = f.relative_to(raw_root)
        save_harmonized(
            weekly=result["weekly"],
            monthly=result["monthly"],
            processed_root=processed_root,
            weekly_folder=settings["weekly_folder"],
            monthly_folder=settings["monthly_folder"],
            rel_path=rel_path,
        )
        logger.info(f"Saved harmonized outputs for {f}")

if __name__ == "__main__":
    main()
