"""
Flickr export preprocessor - extracts ZIP files into metadata and photos directories
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
from pathlib import Path
import shutil
import sys
import zipfile
from typing import List, Dict

from tqdm import tqdm

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.mp4', '.mov', '.avi', '.mkv'}

class FlickrPreprocessor:
    """Handles preprocessing of Flickr export zip files"""

    def __init__(self, source_dir: str, metadata_dir: str, photos_dir: str, quiet: bool = False):
        """
        Initialize the preprocessor

        Args:
            source_dir: Directory containing Flickr export zip files
            metadata_dir: Directory where metadata files should be extracted
            photos_dir: Directory where photos/videos should be extracted
            quiet: Whether to reduce console output
        """
        self.source_dir = Path(source_dir)
        self.metadata_dir = Path(metadata_dir)
        self.photos_dir = Path(photos_dir)
        self.quiet = quiet

        # Initialize statistics
        self.stats = {
            'metadata_files_processed': 0,
            'media_files_processed': 0,
            'errors': [],
            'skipped_files': []
        }

    def _clear_directory(self, directory: Path):
        """Clear all contents of a directory"""
        logging.debug(f"Clearing directory: {directory}")
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)
        logging.debug(f"Directory cleared and recreated: {directory}")

    def _prepare_directories(self):
        """Prepare directories by clearing them and ensuring they exist"""
        self._clear_directory(self.metadata_dir)
        self._clear_directory(self.photos_dir)
        logging.debug("Directories prepared for preprocessing")

    def _is_metadata_zip(self, filename: str) -> bool:
        """Check if a file is a metadata zip file"""
        # Check if filename matches pattern: numbers_alphanumeric_partN.zip
        import re
        pattern = r'\d+_[a-zA-Z0-9]+_part\d+\.zip$'
        return bool(re.match(pattern, filename))

    def _is_media_zip(self, filename: str) -> bool:
        """Check if a file is a media zip file"""
        import re
        patterns = [
            r'^data-download-\d+\.zip$',  # Pattern for photo zip files
            r'^data_\d+_[a-f0-9]+_\d+\.zip$'  # Pattern for alternate format
        ]
        return any(re.match(pattern, filename) for pattern in patterns)

    def _extract_zip(self, zip_path: Path, extract_path: Path, desc: str) -> tuple[int, list[str]]:
        """
        Extract a zip file with progress tracking

        Returns:
            Tuple of (files_processed, error_list)
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get list of files to extract
                files = zip_ref.namelist()
                total_files = len(files)
                processed = 0
                errors = []

                # Extract files with status updates
                print(f"Extracting {desc}")  # Initial message
                for i, file in enumerate(files, 1):
                    try:
                        zip_ref.extract(file, extract_path)
                        processed += 1
                        # Show progress with current filename (truncated if too long)
                        filename = os.path.basename(file)
                        if len(filename) > 30:  # Truncate long filenames
                            filename = filename[:27] + "..."
                        print(f"\rExtracting: {i}/{total_files} ({(i/total_files)*100:.1f}%) - {filename}",
                              end='', flush=True)
                    except Exception as e:
                        errors.append(f"Error extracting {file} from {zip_path.name}: {str(e)}")

                print()  # Final newline
                print(f"Completed: {processed} files extracted")  # Final status

                return processed, errors

        except Exception as e:
            return 0, [f"Error processing {zip_path.name}: {str(e)}"]

    def _process_zip_file(self, zip_path: Path) -> tuple[int, list[str]]:
        """Process a single zip file"""
        if self._is_metadata_zip(zip_path.name):
            return self._extract_zip(zip_path, self.metadata_dir, f"metadata from {zip_path.name}")
        elif self._is_media_zip(zip_path.name):
            return self._extract_zip(zip_path, self.photos_dir, f"media from {zip_path.name}")
        else:
            return 0, [f"Skipped unknown zip file: {zip_path.name}"]

    def process_exports(self):
        """Process all export files in the source directory"""
        try:
            # First clear and prepare directories
            self._prepare_directories()

            # Get list of zip files
            zip_files = [f for f in self.source_dir.iterdir() if f.suffix.lower() == '.zip']
            if not zip_files:
                raise ValueError(f"No zip files found in {self.source_dir}")

            logging.debug(f"Found {len(zip_files)} zip files to process")

            # Process files using thread pool
            with ThreadPoolExecutor(max_workers=min(os.cpu_count(), 4)) as executor:
                # Create future to zip_path mapping
                future_to_zip = {executor.submit(self._process_zip_file, zip_path): zip_path
                               for zip_path in zip_files}

                # Process results as they complete
                for future in tqdm(as_completed(future_to_zip),
                                 total=len(zip_files),
                                 desc="Processing zip files",
                                 unit="files",
                                 disable=False,
                                 file=sys.stdout):
                    zip_path = future_to_zip[future]
                    try:
                        processed, errors = future.result()

                        # Update statistics
                        if self._is_metadata_zip(zip_path.name):
                            self.stats['metadata_files_processed'] += processed
                        elif self._is_media_zip(zip_path.name):
                            self.stats['media_files_processed'] += processed

                        # Record any errors
                        if errors:
                            self.stats['errors'].extend(errors)

                    except Exception as e:
                        self.stats['errors'].append(f"Error processing {zip_path.name}: {str(e)}")

            # Only print statistics if not in quiet mode
            if not self.quiet:
                self._print_statistics()

        except Exception as e:
            logging.error(f"Error during export processing: {str(e)}")
            raise

    def _print_statistics(self):
        """Print processing statistics"""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(formatter)

        # Store and replace handlers temporarily
        original_handlers = logging.getLogger().handlers
        logging.getLogger().handlers = [console_handler]

        try:
            logging.info("\nPreprocessing Results")
            logging.info("-------------------")
            logging.info(f"Metadata files: {self.stats['metadata_files_processed']}")
            logging.info(f"Media files: {self.stats['media_files_processed']}")

            if self.stats['errors']:
                logging.error("\nErrors encountered:")
                for error in self.stats['errors']:
                    logging.error(f"- {error}")

            if self.stats['skipped_files']:
                logging.debug("\nSkipped files:")
                for skipped in self.stats['skipped_files']:
                    logging.debug(f"- {skipped}")
        finally:
            # Restore original handlers
            logging.getLogger().handlers = original_handlers

