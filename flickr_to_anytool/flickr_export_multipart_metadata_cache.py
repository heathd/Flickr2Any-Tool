import json
import logging
from pathlib import Path
from typing import Dict, List, Union


class FlickrExportMultipartMetadataCache:
    def __init__(self, metadata_dir: Path, base_name: str, key: str):
        self.metadata_dir = metadata_dir
        if not self.metadata_dir.exists() or not self.metadata_dir.is_dir():
            raise FileNotFoundError(f"Metadata directory {self.metadata_dir} not found")
        self.base_name = base_name
        self.key = key
        self.data_cache = None

    def data(self) -> Union[Dict, List]:
        if self.data_cache is None:
            self.data_cache = self._load_multipart_json(self.base_name, self.key)
        return self.data_cache

    def albums(self) -> List:
        """Load and parse albums data (single or multi-part)"""
        return self.data()


    def _load_multipart_json(self, base_name: str, key: str) -> Union[Dict, List]:
        """
        Load and merge JSON data from files with various naming patterns

        Args:
            base_name: Base name without extension (e.g., 'albums')
            key: The key in the JSON that contains the data we want

        Returns:
            Either a merged dictionary or list, depending on the data structure
        """
        logging.info(f"Loading {base_name} files...")
        data_files = self._find_json_files(base_name)
        if not data_files:
            return {} if base_name != "albums" else []

        total_files = len(data_files)

        for i, data_file in enumerate(data_files, 1):
            logging.info(f"Processing file {i}/{total_files} ({(i/total_files)*100:.1f}%)")

        try:
            # First file determines the data structure
            with open(data_files[0], 'r', encoding='utf-8') as f:
                first_file_data = json.load(f)
                if key not in first_file_data:
                    logging.warning(f"Key '{key}' not found in first file {data_files[0].name}")
                    return {} if base_name != "albums" else []

                # Initialize with appropriate type
                if isinstance(first_file_data[key], dict):
                    merged_data = {}
                else:  # List type
                    merged_data = []

            # Now process all files
            for data_file in data_files:
                logging.info(f"Processing {data_file.name}")
                with open(data_file, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
                    if key in file_data:
                        if isinstance(merged_data, dict):
                            # Dictionary merge
                            merged_data.update(file_data[key])
                        else:
                            # List merge
                            merged_data.extend(file_data[key])
                        logging.info(f"Loaded {len(file_data[key])} entries from {data_file.name}")
                    else:
                        logging.warning(f"Key '{key}' not found in {data_file.name}")

            count = len(merged_data) if isinstance(merged_data, (dict, list)) else 1
            files_word = "files" if len(data_files) > 1 else "file"
            logging.info(f"Loaded total {count} entries from {len(data_files)} {base_name} {files_word}")
            return merged_data

        except Exception as e:
            logging.error(f"Error loading {base_name}: {str(e)}")
            return {} if base_name != "albums" else []


    def _find_json_files(self, base_name: str) -> List[Path]:
        """Find JSON files matching various patterns"""
        # Move debug logging to logging.debug level
        logging.debug(f"Searching for {base_name} files")

        patterns = [
            f"{base_name}.json",
            f"{base_name}_part*.json",
            f"{base_name}*.json"
        ]

        matching_files = []
        for pattern in patterns:
            files = list(self.metadata_dir.glob(pattern))
            if files:
                logging.debug(f"Found {len(files)} files matching pattern {pattern}")
                for f in files:
                    logging.debug(f"  - {f.name}")
                matching_files.extend(files)

        unique_files = sorted(set(matching_files))

        if not unique_files:
            logging.debug(f"No {base_name} files found with any pattern")
        else:
            logging.debug(f"Found {len(unique_files)} unique {base_name} files")

        return unique_files
