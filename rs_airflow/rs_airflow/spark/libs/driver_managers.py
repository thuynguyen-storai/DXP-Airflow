import logging
import pathlib


class DriverManager:
    @classmethod
    def get_drivers_path_list(cls) -> str:
        current_file_path = pathlib.Path(__file__)

        driver_folder_dir = current_file_path / ".." / ".." / "drivers"
        driver_paths = driver_folder_dir.resolve().rglob("*.jar")

        driver_absolute_paths = [str(path.resolve()) for path in driver_paths]

        join_driver_paths = str.join(",", driver_absolute_paths)

        logging.getLogger(__name__).info("Loaded drivers: %s", join_driver_paths)
        return join_driver_paths
