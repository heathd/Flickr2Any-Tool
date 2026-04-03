from typing import Dict
import logging
import mimetypes
import os
from pathlib import Path
import re
from typing import Optional

from flickr_to_anytool.constants import MediaType


class OutputHelpers:

    @staticmethod
    def sanitize_folder_name(name: str) -> str:
        """Convert filename/foldername to safe version"""
        # Replace spaces with underscores and remove/replace special characters
        sanitized = "".join(c if c.isalnum() or c in ('_', '-') else '_'
                        for c in name.replace(' ', '_'))

        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)

        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')

        # Ensure name isn't empty
        if not sanitized:
            sanitized = "unnamed"
        return sanitized

    @staticmethod
    def get_destination_filename(photo_id: str, source_file: Path, photo_json: Optional[Dict]) -> str:
        """Get the destination filename for a photo, without Flickr ID"""
        try:
            # Get original name from metadata if available
            if photo_json and photo_json.get('name'):
                original_name = photo_json['name'].strip()
                # Ensure name isn't empty after stripping
                if not original_name:
                    original_name = source_file.name
            else:
                # Use the original filename
                original_name = source_file.name

            # Remove all Flickr IDs from filename
            # Split name and extension
            name_base, ext = os.path.splitext(original_name)

            # Remove any trailing Flickr ID patterns
            # Common patterns: name_123456789 or name_123456789_o
            import re
            # Remove ID patterns like _123456789 or _123456789_o
            name_base = re.sub(r'_\d{8,11}(?:_o)?$', '', name_base)

            # In case there are multiple IDs, keep removing them
            while re.search(r'_\d{8,11}(?:_o)?', name_base):
                name_base = re.sub(r'_\d{8,11}(?:_o)?', '', name_base)

            # Reconstruct filename
            dest_filename = f"{name_base}{ext}"

            # Ensure extension is present
            if not dest_filename.lower().endswith(source_file.suffix.lower()):
                dest_filename = f"{dest_filename}{source_file.suffix}"

            # Sanitize the filename
            return OutputHelpers.sanitize_filename(dest_filename)

        except Exception as e:
            logging.error(f"Error creating destination filename for {photo_id}: {str(e)}")
            # Fallback to a safe filename without ID
            name_base = source_file.stem
            # Remove any Flickr IDs from the fallback name
            name_base = re.sub(r'_\d{8,11}(?:_o)?', '', name_base)
            return f"{name_base}{source_file.suffix}"

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Convert filename to filesystem-safe version"""
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')

        # Remove leading/trailing spaces and periods
        filename = filename.strip('. ')

        # Replace multiple spaces/underscores with single underscore
        filename = re.sub(r'[\s_]+', '_', filename)

        # Ensure filename isn't empty
        if not filename or filename.startswith('.'):
            return 'untitled_photo'

        # Limit filename length (max 255 chars is common filesystem limit)
        if len(filename) > 255:
            name, ext = os.path.splitext(filename)
            filename = name[:255 - len(ext)] + ext

        return filename

    @staticmethod
    def get_media_type(file_path: Path) -> MediaType:
        """Determine the type of media file"""
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if mime_type:
            if mime_type.startswith('image/'):
                return MediaType.IMAGE
            elif mime_type.startswith('video/'):
                return MediaType.VIDEO
        return MediaType.UNKNOWN
