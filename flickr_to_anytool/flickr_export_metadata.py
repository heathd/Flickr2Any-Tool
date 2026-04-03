from typing import Dict, Optional
import json
import logging
from pathlib import Path

class FlickrExportMetadata:
    def __init__(self, metadata_dir: Path):
        self.metadata_dir = metadata_dir
        if self.metadata_dir is None or not self.metadata_dir.exists() or not self.metadata_dir.is_dir():
            raise FileNotFoundError(f"Metadata directory {self.metadata_dir} not found")
        self.cache: Dict[str, Optional[Dict]] = {}

    def get(self, photo_id: str) -> Optional[Dict]:
        """Get metadata for a photo ID, using cache if available"""
        if photo_id not in self.cache:
            self.cache[photo_id] = self.get_uncached(photo_id)

        return self.cache[photo_id]

    def get_uncached(self, photo_id: str) -> Optional[Dict]:
        """Load metadata for a photo, handling multiple possible IDs"""
        candidate_files = [
            f"photo_{photo_id}.json",
            f"{photo_id}.json",
            f"{int(photo_id):d}.json"
        ]

        for file in candidate_files:
            path = self.metadata_dir / file
            if path.exists():
                return self.read_metadata_file(path)

        maybe_file = self.find_metadata_by_scanning_content_of_json_files(photo_id)
        if maybe_file:
            return self.read_metadata_file(maybe_file)

        logging.debug(f"No metadata found for photo {photo_id}")
        return None

    def read_metadata_file(self, file: str) -> Optional[Dict]:
        """Read metadata from a specific JSON file"""
        try:
            with open(file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error reading metadata file {file}: {str(e)}")
            return None

    def find_metadata_by_scanning_content_of_json_files(self, photo_id: str) -> Optional[Dict]:
        """Scan all JSON files for potential matches based on content"""
        # If no direct match, look for files that might contain this as original ID
        all_metadata_files = list(self.metadata_dir.glob("photo_*.json"))
        for mfile in all_metadata_files:
            try:
                with open(mfile, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Check various fields where the ID might appear
                    if (str(data.get('id', '')) == photo_id or
                        str(data.get('original_id', '')) == photo_id or
                        photo_id in [str(x) for x in data.get('related_ids', [])] or
                        photo_id in str(data.get('original', ''))):
                        return data
            except:
                continue

        logging.debug(f"No metadata found for photo {photo_id} after scanning all JSON files")
        return None
