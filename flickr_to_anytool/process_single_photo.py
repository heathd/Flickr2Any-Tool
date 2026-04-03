from datetime import datetime
import logging
from pathlib import Path
import shutil
import traceback
from flickr_to_anytool.flickr_export_metadata import FlickrExportMetadata
from typing import Dict, Optional, Tuple

from flickr_to_anytool.exif_writer import ExifWriter
from flickr_to_anytool.output_helpers import OutputHelpers

from .constants import MediaType

class ProcessSinglePhoto:

    def __init__(self, photo_id_map: Dict[str, str], exif_writer: ExifWriter, flickr_export_metadata: FlickrExportMetadata, failed_dir, date_format, write_xmp_sidecars):
        self.photo_id_map = photo_id_map
        self.flickr_export_metadata = flickr_export_metadata
        self.failed_dir = failed_dir
        self.date_format = date_format
        self.write_xmp_sidecars = write_xmp_sidecars
        self.exif_writer: ExifWriter = exif_writer


    #by date processing method
    def _process_single_photo_by_date(self, photo_id: str, working_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
        """Process a single photo with date-based organization"""
        try:
            # Use pre-loaded maps
            source_file = self.photo_id_map.get(photo_id)
            if not source_file:
                return self._handle_failed_file(photo_id, None, "Source file not found")

            photo_json = self.flickr_export_metadata.get(photo_id)

            if not photo_json:
                return self._handle_failed_file(photo_id, source_file, "No metadata found")

            # Get date and create directory
            try:
                if photo_json and 'date_taken' in photo_json:
                    date_taken = photo_json['date_taken']
                else:
                    date_taken = datetime.fromtimestamp(source_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                date_taken = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            date_path = self._get_date_path(date_taken, self.date_format)
            date_dir = working_dir / date_path
            date_dir.mkdir(parents=True, exist_ok=True)

            dest_file = date_dir / OutputHelpers.get_destination_filename(photo_id, source_file, photo_json)

            # Process file
            shutil.copy2(source_file, dest_file)

            if photo_json:
                media_type = OutputHelpers.get_media_type(dest_file)
                if media_type == MediaType.IMAGE:
                    self.exif_writer._embed_image_metadata(dest_file, photo_json)
                    if self.write_xmp_sidecars:
                        self.exif_writer._write_xmp_sidecar(dest_file, photo_json)
                elif media_type == MediaType.VIDEO:
                    self.exif_writer._embed_video_metadata(dest_file, photo_json)
                    if self.write_xmp_sidecars:
                        self.exif_writer._write_xmp_sidecar(dest_file, photo_json)

            return source_file, dest_file

        except Exception as e:
            return self._handle_failed_file(photo_id, source_file if 'source_file' in locals() else None,
                                          f"Processing error: {str(e)}")

    #by album processing method
    def _process_single_photo(self, photo_id, albums, working_dir) -> Tuple[Optional[Path], Optional[Path]]:
        """Process a single photo and copy to all its album locations"""
        try:
            if not working_dir:
                return self._handle_failed_file(photo_id, None, "No working directory")

            # Use pre-loaded maps
            source_file = self.photo_id_map.get(photo_id)
            if not source_file:
                return self._handle_failed_file(photo_id, None, "Source file not found")

            photo_json = self.flickr_export_metadata.get(photo_id)
            if not photo_json:
                return self._handle_failed_file(photo_id, source_file, "No metadata found")

            # Process once for all albums
            processed_files = []
            for album_name in albums:
                try:
                    # Determine album directory
                    if album_name == '00_NoAlbum':
                        date_taken = photo_json.get('date_taken')
                        if not date_taken:
                            date_taken = datetime.fromtimestamp(source_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        date_path = self._get_date_path(date_taken, self.date_format)
                        album_dir = working_dir / "00_NoAlbum" / date_path
                    else:
                        album_dir = working_dir / OutputHelpers.sanitize_folder_name(album_name)

                    album_dir.mkdir(parents=True, exist_ok=True)
                    dest_file = album_dir / OutputHelpers.get_destination_filename(photo_id, source_file, photo_json)

                    # Copy file
                    shutil.copy2(source_file, dest_file)

                    # Only embed metadata for the first copy
                    if not processed_files:
                        media_type = OutputHelpers.get_media_type(dest_file)
                        if media_type == MediaType.IMAGE:
                            self._embed_image_metadata(dest_file, photo_json)
                            if self.write_xmp_sidecars:
                                self.exif_writer._write_xmp_sidecar(dest_file, photo_json)
                        elif media_type == MediaType.VIDEO:
                            self._embed_video_metadata(dest_file, photo_json)
                            if self.write_xmp_sidecars:
                                self.exif_writer._write_xmp_sidecar(dest_file, photo_json)

                    processed_files.append(dest_file)

                except Exception as e:
                    traceback.print_exception(e)
                    return self._handle_failed_file(photo_id, source_file, f"Album processing error: {str(e)}")

            return source_file, processed_files[0] if processed_files else None

        except Exception as e:
            traceback.print_stack()
            return self._handle_failed_file(photo_id, source_file if 'source_file' in locals() else None,
                                          f"Processing error: {str(e)}")


    def _handle_failed_file(self, photo_id: str, source_file: Optional[Path], error_reason: str) -> Tuple[Optional[Path], Optional[Path]]:
        """Handle a failed file by copying it to the failed directory and logging the error"""
        try:
            if source_file and source_file.exists():
                # Create failed_files directory
                self.failed_dir.mkdir(parents=True, exist_ok=True)

                # Create destination path using same date structure as main export
                dest_file = self.failed_dir / source_file.name

                # Handle filename conflicts
                counter = 1
                while dest_file.exists():
                    dest_file = self.failed_dir / f"{source_file.stem}_{counter}{source_file.suffix}"
                    counter += 1

                # Copy the file
                try:
                    shutil.copy2(source_file, dest_file)

                    # Create an error info file
                    error_info_file = dest_file.with_suffix('.error.txt')
                    with open(error_info_file, 'w', encoding='utf-8') as f:
                        f.write(f"Photo ID: {photo_id}\n")
                        f.write(f"Original path: {source_file}\n")
                        f.write(f"Error: {error_reason}\n")
                        f.write(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

                    return source_file, dest_file

                except Exception as copy_error:
                    print(f"Error copying failed file: {str(copy_error)}")
                    return None, None

        except Exception as e:
            print(f"Error handling failed file {photo_id}: {str(e)}")

        return None, None



    def _get_date_path(self, date_str: str, date_format: str) -> Path:
        """Convert date string to folder path based on format"""
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

            if date_format == 'yyyy/yyyy-mm-dd':
                return Path(f"{date.year}/{date.year:04d}-{date.month:02d}-{date.day:02d}")
            elif date_format == 'yyyy/yyyy-mm':
                return Path(f"{date.year}/{date.year:04d}-{date.month:02d}")
            elif date_format == 'yyyy-mm-dd':
                return Path(f"{date.year:04d}-{date.month:02d}-{date.day:02d}")
            elif date_format == 'yyyy-mm':
                return Path(f"{date.year:04d}-{date.month:02d}")
            elif date_format == 'yyyy':
                return Path(f"{date.year:04d}")
            else:
                raise ValueError(f"Unsupported date format: {date_format}")

        except Exception as e:
            logging.error(f"Error processing date {date_str}: {str(e)}")
            return Path("unknown_date")


