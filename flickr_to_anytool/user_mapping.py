import logging
from pathlib import Path
from typing import Dict

from flickr_to_anytool.flickr_export_multipart_metadata_cache import FlickrExportMultipartMetadataCache


class UserMapping:
    def __init__(self, metadata_dir: Path):
        self.metadata_dir = metadata_dir
        self.contacts = FlickrExportMultipartMetadataCache("contacts", "contacts").data()
        self.user_mapping = None

    def get_user_mapping(self) -> Dict[str, str]:
        """Get the user mapping, processing it if not already done"""
        if self.user_mapping is None:
            self._process_user_mappings()
        return self.user_mapping

    def _process_user_mappings(self):
        """Process user mappings from loaded contacts data"""
        try:
            for username, url in self.contacts.items():
                # Extract user ID from URL, handling different URL formats
                if '/people/' in url:
                    user_id = url.split('/people/')[1].strip('/')
                else:
                    user_id = url.strip()

                # Store both the full ID and the cleaned version (without @N00 if present)
                self.user_mapping[user_id] = username
                if '@' in user_id:
                    clean_id = user_id.split('@')[0]
                    self.user_mapping[clean_id] = username

            logging.info(f"Processed {len(self.user_mapping)} user mappings")
            # Log first few mappings to help with debugging
            sample_mappings = dict(list(self.user_mapping.items())[:3])
            logging.info(f"Sample user mappings: {sample_mappings}")
        except Exception as e:
            logging.error(f"Error processing user mappings: {str(e)}")
