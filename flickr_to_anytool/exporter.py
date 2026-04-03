"""
Flickr to Immich export handler - main conversion logic
"""

from pathlib import Path
from typing import Dict, List, Optional
import json
import logging
import shutil
import time
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import gc
import psutil

from flickr_to_anytool.exif_writer import ExifWriter
from flickr_to_anytool.flickr_api_metadata import FlickrApiMetadata
from flickr_to_anytool.flickr_export_metadata import FlickrExportMetadata
from flickr_to_anytool.flickr_export_multipart_metadata_cache import FlickrExportMultipartMetadataCache
from flickr_to_anytool.output_helpers import OutputHelpers
from flickr_to_anytool.process_single_photo import ProcessSinglePhoto

from .constants import INCLUDE_EXTENDED_DESCRIPTION, WRITE_XMP_SIDECARS

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.mp4', '.mov', '.avi', '.mkv'}

class FlickrToImmich:

    logging.getLogger().setLevel(logging.WARNING)

    # Modified SUPPORTED_EXTENSIONS initialization
    SUPPORTED_EXTENSIONS = {
        ext.lower() for ext in {
            # Images
            '.jpg', '.jpeg', '.png', '.gif', '.tiff', '.tif', '.webp',
            '.JPG', '.JPEG', '.PNG', '.GIF', '.TIFF', '.TIF', '.WEBP',
            # Videos
            '.mp4', '.mov', '.avi', '.mpg', '.mpeg', '.m4v', '.3gp', '.wmv',
            '.webm', '.mkv', '.flv',
            '.MP4', '.MOV', '.AVI', '.MPG', '.MPEG', '.M4V', '.3GP', '.WMV',
            '.WEBM', '.MKV', '.FLV'
        }
    }

    def _is_supported_extension(self, filename: str) -> bool:
        """Check if a file has a supported extension, case-insensitive"""
        return any(filename.lower().endswith(ext) for ext in self.SUPPORTED_EXTENSIONS)

    def _clear_output_directory(self):
        """Clear all contents of a directory"""
        try:
            if self.output_dir.exists():
                shutil.rmtree(self.output_dir)
            self.output_dir.mkdir(parents=True)
            logging.debug(f"Directory cleared and recreated: {self.output_dir}")
        except Exception as e:
            raise ValueError(f"Error clearing output directory: {str(e)}")



    def _find_unorganized_photos(self) -> Dict[str, Path]:
        """
        Find all photos in the photos directory that aren't in any album
        Returns a dict mapping photo IDs to their file paths
        """
        try:
            # Get all media files in photos directory
            all_photos = {}  # photo_id -> Path
            unidentified_photos = []  # Files where we couldn't extract an ID

            logging.debug("\n=== Starting Unorganized Photos Analysis ===")

            # List all files in directory
            all_files = list(self.photos_dir.iterdir())
            logging.debug(f"Found {len(all_files)} total files in photos directory")

            # First, log what's in photo_to_albums
            logging.debug("\nCurrently organized photos:")
            logging.debug(f"photo_to_albums contains {len(self.photo_to_albums)} entries")
            for photo_id in list(self.photo_to_albums.keys())[:5]:  # Show first 5
                logging.debug(f"- Photo ID {photo_id} is in albums: {self.photo_to_albums[photo_id]}")

            logging.info("Scanning photos directory for unorganized photos...")

            # Process each file
            logging.debug("\nProcessing files in photos directory:")
            for file in all_files:
                if not file.is_file():
                    logging.debug(f"Skipping non-file: {file.name}")
                    continue

                if not self._is_supported_extension(file.name):
                    logging.debug(f"Skipping unsupported extension: {file.name}")
                    continue

                photo_id = self._extract_photo_id(file.name)
                if photo_id:
                    logging.debug(f"Found photo ID {photo_id} for file: {file.name}")
                    all_photos[photo_id] = file
                else:
                    logging.debug(f"Could not extract photo ID from: {file.name}")
                    unidentified_photos.append(file)

            # For files without IDs, generate sequential IDs
            for idx, file in enumerate(unidentified_photos):
                generated_id = f"unknown_{idx+1}"
                all_photos[generated_id] = file
                logging.info(f"Generated ID {generated_id} for file: {file.name}")

            # Find photos not in any album
            organized_photos = set(self.photo_to_albums.keys())
            unorganized_photos = {}

            logging.debug("\nComparing with organized photos:")
            logging.debug(f"Total photos found: {len(all_photos)}")
            logging.debug(f"Photos in albums: {len(organized_photos)}")

            for photo_id, file_path in all_photos.items():
                if photo_id not in organized_photos:
                    unorganized_photos[photo_id] = file_path
                    logging.debug(f"Found unorganized photo: {file_path.name} (ID: {photo_id})")

            # Enhanced logging
            if unorganized_photos:
                logging.info(f"Found {len(unorganized_photos)} photos not in any album")
                logging.info("Sample of unorganized photos:")
                for photo_id, file_path in list(unorganized_photos.items())[:5]:
                    logging.info(f"  - {photo_id}: {file_path.name}")

                # Log some statistics
                logging.info(f"\nPhoto Organization Statistics:")
                logging.info(f"Total files processed: {len(all_photos)}")
                logging.info(f"Files with extracted IDs: {len(all_photos) - len(unidentified_photos)}")
                logging.info(f"Files needing generated IDs: {len(unidentified_photos)}")
                logging.info(f"Organized photos: {len(organized_photos)}")
                logging.info(f"Unorganized photos: {len(unorganized_photos)}")
            else:
                logging.info("All photos are organized in albums")

            logging.debug("=== End Unorganized Photos Analysis ===\n")
            return unorganized_photos

        except Exception as e:
            logging.error(f"Error finding unorganized photos: {str(e)}")
            logging.exception("Full traceback:")
            return {}

    def __init__(self,
                    metadata_dir: str,
                    photos_dir: str,
                    output_dir: str,
                    date_format: str = 'yyyy/yyyy-mm-dd',
                    api_key: Optional[str] = None,
                    api_secret: Optional[str] = None,
                    log_file: Optional[str] = None,
                    results_dir: Optional[str] = None,
                    include_extended_description: bool = INCLUDE_EXTENDED_DESCRIPTION,
                    write_xmp_sidecars: bool = WRITE_XMP_SIDECARS,
                    block_if_failure: bool = False,
                    resume: bool = False,
                    use_api: bool = False,
                    quiet: bool = False,
                    debug: bool = False,
                    fave_weight: float = 10.0,
                    comment_weight: float = 3.0,
                    view_weight: float = 5.0,
                    min_views: int = 10,
                    min_faves: int = 2,
                    min_comments: int = 0,
                    cpu_cores: int = 4,
                    batch_size: int = 1000,
                    max_memory_percent: int = 75):

            """
            Initialize the converter with source and destination directories

            Args:
                metadata_dir: Directory containing Flickr export JSON files
                photos_dir: Directory containing the photos and videos
                output_dir: Directory where the album structure will be created
                api_key: Optional Flickr API key for additional metadata
                log_file: Optional path to log file
                include_extended_description: Whether to include extended metadata in description
                write_xmp_sidecars: Whether to write XMP sidecar files
                quiet: Whether to reduce console output (default: False)
            """

            # First, set all parameters as instance attributes
            self.block_if_failure = block_if_failure
            self.resume = resume
            self.date_format = date_format  # Now this will always have a value
            self.quiet = quiet
            self.use_api = use_api
            total_memory = psutil.virtual_memory().total
            self.max_memory = (total_memory * max_memory_percent) // 100
            self.batch_size = batch_size
            self.cpu_cores = cpu_cores
            self.include_extended_description = include_extended_description
            self.write_xmp_sidecars = write_xmp_sidecars

            logging.info(f"Maximum memory set to {self.max_memory / (1024*1024*1024):.1f}GB "
                        f"({max_memory_percent}% of system memory)")
            logging.info(f"Batch size set to {batch_size} photos")
            logging.info(f"Using {cpu_cores} CPU cores")

            # Track processing statistics
            self.stats = {
                'total_files': 0,
                'successful': {
                    'count': 0,
                    'details': set()  # Will store (source_file, dest_file, status) tuples
                },
                'failed': {
                    'count': 0,
                    'details': [],  # Top-level details for overall failures
                    'metadata': {
                        'count': 0,
                        'details': set()  # Will store (file, error_msg, exported) tuples
                    },
                    'file_copy': {
                        'count': 0,
                        'details': set()  # Will store (file, error_msg) tuples
                    }
                },
                'skipped': {
                    'count': 0,
                    'details': set()  # Will store (file, reason) tuples
                },
                'errors': []
            }

            # Setup logging with quiet parameter
            self._setup_logging(log_file, quiet, debug)

            # Initialize directories
            self.metadata_dir = Path(metadata_dir)
            self.photos_dir = Path(photos_dir)
            self.output_dir = Path(output_dir)
            self.results_dir = Path(results_dir) if results_dir else self.output_dir

            self.flickr_export_metadata: FlickrExportMetadata = FlickrExportMetadata(self.metadata_dir)

            # Clear output directory if not resuming
            if not self.resume:
                self._clear_output_directory()

            # Add tracking sets
            self.processed_ids = set()  # Track unique photo IDs
            self.successful_ids = set()  # Track successfully processed IDs
            self.failed_ids = set()     # Track failed IDs

            # Create failed files directory
            self.failed_dir = self.output_dir / "failed_files"
            self.failed_dir.mkdir(parents=True, exist_ok=True)

            # Initialize data containers
            self.account_data = None
            self.user_mapping = {}
            self.photo_to_albums: Dict[str, List[str]] = {}

            #interestingness / hightlights
            self.fave_weight = fave_weight
            self.comment_weight = comment_weight
            self.view_weight = view_weight
            self.min_views = min_views
            self.min_faves = min_faves
            self.min_comments = min_comments

            # Validate directories
            self._validate_directories()

            # Initialize Flickr API if key is provided
            self.flickr_wrapper = FlickrApiMetadata.auth(api_key, api_secret)

            try:
                # Debug: List ALL files in metadata directory
                logging.info("=== DIRECTORY CONTENTS ===")
                logging.info(f"Metadata directory: {self.metadata_dir}")
                for file in self.metadata_dir.iterdir():
                    logging.info(f"Found file: {file.name}")
                logging.info("=== END DIRECTORY CONTENTS ===")

                # Load account profile (single file)
                self._load_account_profile()

                # Load all data, handling both single and multi-part files
                # self.comments = FlickrExportMultipartMetadataCache(self.metadata_dir, "comments", "comments").data()
                # self.favorites = FlickrExportMultipartMetadataCache(self.metadata_dir, "faves", "faves").data()
                # self.followers = FlickrExportMultipartMetadataCache(self.metadata_dir, "followers", "followers").data()
                # self.galleries = FlickrExportMultipartMetadataCache(self.metadata_dir, "galleries", "galleries").data()
                # self.apps_comments = FlickrExportMultipartMetadataCache(self.metadata_dir, "apps_comments", "comments").data()
                # self.gallery_comments = FlickrExportMultipartMetadataCache(self.metadata_dir, "galleries_comments", "comments").data()

                # Load albums (could be single or multi-part)
                self.albums = FlickrExportMultipartMetadataCache(self.metadata_dir, "albums", "albums").data()
                if not isinstance(self.albums, list):
                    logging.error("Albums data is not in expected list format")
                    raise ValueError("Invalid albums data format")

                # Build photo -> album mapping
                self._build_photo_album_mapping()

                # Just build photo ID map
                print("Building photo index...")
                #print(f"Photos directory: {photos_dir}")  # Add this line
                self.photo_id_map = {}
                photo_files = [f for f in self.photos_dir.iterdir() if f.is_file()]
                #print(f"Found {len(photo_files)} files in directory")  # Add this line
                for f in photo_files:
                    photo_id = self._extract_photo_id(f.name)
                    if photo_id:
                        self.photo_id_map[photo_id] = f
                    else:
                        print(f"Could not extract ID from: {f.name}")  # Add this line

                print(f"Successfully indexed {len(self.photo_id_map)} photos")  # Add this line

                # Initialize tracking sets
                self.unique_successful = set()
                self.unique_failed = set()

            except Exception as e:
                logging.error(f"Initialization failed: {str(e)}")
                raise

    def _setup_logging(self, log_file: Optional[str], quiet: bool, debug: bool):
        """Configure logging with both file and console output"""


        if debug:
            log_level = logging.DEBUG
        elif quiet:
            log_level = logging.WARNING
        else:
            log_level = logging.INFO

        # Set up root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)  # Set to DEBUG to capture all levels

        # Remove any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Set up format for logging
        console_format = '%(levelname)s - %(message)s'
        file_format = '%(asctime)s - %(levelname)s - %(message)s'

        # Set up minimal console output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        root_logger.addHandler(console_handler)

        # Only set up file logging if specifically requested
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.ERROR)  # Only log errors to file
            root_logger.addHandler(file_handler)

        # Reduce logging from external libraries
        logging.getLogger('flickrapi').setLevel(logging.CRITICAL)
        logging.getLogger('requests').setLevel(logging.CRITICAL)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)
        logging.getLogger('PIL').setLevel(logging.CRITICAL)

        # Log that debug logging is enabled
        logging.debug("Debug logging initialized")

    def write_results_log(self):
        """Write a detailed results log file with enhanced failure reporting"""
        self.results_dir.mkdir(parents=True, exist_ok=True)
        results_file = self.results_dir / 'processing_results.txt'

        try:
            with open(results_file, 'w', encoding='utf-8') as f:
                f.write("FLICKR TO IMMICH PROCESSING RESULTS\n")
                f.write("=================================\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                # Write summary counts
                f.write("SUMMARY\n-------\n")
                f.write(f"Total photos/videos processed: {self.stats['total_files']}\n")
                f.write(f"Successfully processed files: {self.stats['successful']['count']}\n")
                f.write(f"Failed metadata: {self.stats['failed']['metadata']['count']}\n")
                f.write(f"Failed file copy: {self.stats['failed']['file_copy']['count']}\n")
                f.write(f"Skipped: {self.stats['skipped']['count']}\n")
                if 'partial_metadata' in self.stats:
                    f.write(f"Partial metadata success: {self.stats['partial_metadata']['count']}\n")
                f.write("\n")

                # Write successful files
                f.write("SUCCESSFUL FILES\n----------------\n")
                for source, dest, status in self.stats['successful']['details']:
                    f.write(f"Source: {source}\n")
                    f.write(f"Destination: {dest}\n")
                    f.write(f"Status: {status}\n")
                    f.write("-" * 50 + "\n")

                # Write partial metadata successes if any
                if 'partial_metadata' in self.stats and self.stats['partial_metadata']['files']:
                    f.write("\nPARTIAL METADATA SUCCESS\n----------------------\n")
                    f.write("These files were exported but only with basic metadata:\n")
                    for file in self.stats['partial_metadata']['files']:
                        f.write(f"File: {file}\n")
                    f.write("-" * 50 + "\n")

                # Write metadata failures
                f.write("\nMETADATA FAILURES\n-----------------\n")
                for file, error, exported in self.stats['failed']['metadata']['details']:
                    f.write(f"File: {file}\n")
                    f.write(f"Error: {error}\n")
                    f.write(f"File exported: {'Yes' if exported else 'No'}\n")
                    f.write("-" * 50 + "\n")

                # Write file copy failures
                f.write("\nFILE COPY FAILURES\n------------------\n")
                for file, error in self.stats['failed']['file_copy']['details']:
                    f.write(f"File: {file}\n")
                    f.write(f"Error: {error}\n")
                    f.write("-" * 50 + "\n")

                # Write skipped files
                f.write("\nSKIPPED FILES\n-------------\n")
                for file, reason in self.stats['skipped']['details']:
                    f.write(f"File: {file}\n")
                    f.write(f"Reason: {reason}\n")
                    f.write("-" * 50 + "\n")

            logging.info(f"Results log written to {results_file}")

        except Exception as e:
            logging.error(f"Error writing results log: {str(e)}")

    def _load_account_profile(self):
        """Load the account profile data with progress indication"""
        with tqdm(total=100, desc="Loading account profile", unit="%") as pbar:
            profile_file = self.metadata_dir / 'account_profile.json'
            pbar.update(20)  # Show progress for finding file

            try:
                if not profile_file.exists():
                    logging.warning("account_profile.json not found, some metadata will be missing")
                    self.account_data = {}
                    pbar.update(80)  # Complete the progress bar
                    return

                pbar.update(30)  # Show progress for file check

                with open(profile_file, 'r', encoding='utf-8') as f:
                    self.account_data = json.load(f)
                    pbar.update(40)  # Show progress for loading

                    user_name = self.account_data.get('real_name', 'unknown user')
                    logging.info(f"Loaded account profile for {user_name}")

                    # Log some account details
                    logging.debug("Account details loaded:")
                    logging.debug(f"- Username: {self.account_data.get('screen_name', 'unknown')}")
                    logging.debug(f"- Join date: {self.account_data.get('join_date', 'unknown')}")
                    logging.debug(f"- NSID: {self.account_data.get('nsid', 'unknown')}")

                    pbar.update(10)  # Final progress update

            except json.JSONDecodeError as e:
                logging.error(f"Error decoding account profile JSON: {str(e)}")
                self.account_data = {}
                pbar.update(100)  # Complete the progress bar

            except Exception as e:
                logging.error(f"Error loading account profile: {str(e)}")
                self.account_data = {}
                pbar.update(100)  # Complete the progress bar

    def _validate_directories(self):
        """Validate input and output directories"""
        if not self.metadata_dir.exists():
            raise ValueError(f"Metadata directory does not exist: {self.metadata_dir}")

        if not self.photos_dir.exists():
            raise ValueError(f"Photos directory does not exist: {self.photos_dir}")

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _extract_photo_id(self, filename: str) -> Optional[str]:
        """
        Extract photo ID from filename and verify against metadata existence.
        Returns the ID that has matching metadata.
        """
        #print(f"Attempting to extract ID from: {filename}")  # Add this line
        # Convert filename to lowercase for consistent matching
        filename = filename.lower()

        # Find ALL potential IDs in the filename, including those not following _o pattern
        import re
        # Updated pattern to catch more ID variations
        patterns = [
            r'_(\d{8,11})(?:_o)?(?:\.|_)',  # Standard pattern
            r'[^0-9](\d{8,11})[^0-9]',      # Any 8-11 digit number
        ]

        all_matches = set()
        for pattern in patterns:
            matches = re.findall(pattern, filename)
            all_matches.update(matches)

        if not all_matches:
            return None

        # Convert matches to list and sort by length (to prioritize consistent length IDs)
        matches = sorted(all_matches, key=len, reverse=True)

        if len(matches) >= 1:
            # First, try to find metadata for any of the IDs
            for photo_id in matches:
                if self._find_metadata_file(photo_id):
                    if len(matches) > 1:
                        logging.debug(f"Multiple IDs in {filename}, using {photo_id} (found metadata)")
                    return photo_id

            # If no metadata found but we have multiple IDs, use the largest (most recent) one
            if len(matches) > 1:
                largest_id = str(max(int(id_) for id_ in matches))
                logging.debug(f"No metadata found for any ID in {filename}, using largest: {largest_id}")
                return largest_id

            # If single ID, return it even if no metadata (for consistent behavior)
            return matches[0]

        return None


    def _find_metadata_file(self, photo_id: str) -> Optional[Path]:
        """Helper method to find metadata file for a given ID"""
        patterns = [
            f"photo_{photo_id}.json",
            f"{photo_id}.json"
        ]

        for pattern in patterns:
            metadata_file = self.metadata_dir / pattern
            if metadata_file.exists():
                return metadata_file
        return None

    def _build_photo_album_mapping(self):
        """Build mapping of photos to their albums"""
        try:
            for album in self.albums:
                if 'photos' not in album:
                    logging.warning(f"Album '{album.get('title', 'Unknown')}' has no photos key")
                    continue

                for photo_id in album['photos']:
                    if photo_id not in self.photo_to_albums:
                        self.photo_to_albums[photo_id] = []
                    self.photo_to_albums[photo_id].append(album['title'])

            # Find unorganized photos and add to '00_NoAlbum'
            unorganized_photos = self._find_unorganized_photos()
            if unorganized_photos:
                # Create the 'No Album' album if unorganized photos exist
                no_album_album = {
                    'title': '00_NoAlbum',
                    'description': 'Photos not organized in any Flickr album',
                    'photos': []
                }
                self.albums.append(no_album_album)

                # Add each unorganized photo to the mapping and album
                for photo_id, file_path in unorganized_photos.items():
                    if photo_id not in self.photo_to_albums:
                        self.photo_to_albums[photo_id] = ['00_NoAlbum']
                        no_album_album['photos'].append(photo_id)  # Add to the album's photo list

            logging.info(f"Photo-album mapping complete. Total unique media items: {len(self.photo_to_albums)}")
        except Exception as e:
            raise ValueError(f"Error building photo-album mapping: {str(e)}")


    def create_album_structure(self) -> Path:
        """Create the album folder structure and return working directory"""
        try:
            full_export_dir = self.output_dir / "full_library_export" / "by_album"
            print(f"Creating album structure in: {full_export_dir}")
            sys.stdout.flush()

            # Create the directory
            full_export_dir.mkdir(parents=True, exist_ok=True)

            # Verify directory exists
            if not full_export_dir.exists():
                raise ValueError(f"Failed to create directory: {full_export_dir}")

            # Create album directories under by_album
            if hasattr(self, 'albums'):
                for album in self.albums:
                    album_dir = full_export_dir / OutputHelpers.sanitize_folder_name(album['title'])
                    album_dir.mkdir(parents=True, exist_ok=True)

                    if not album_dir.exists():
                        raise ValueError(f"Failed to create album directory: {album_dir}")

                print(f"Created {len(self.albums)} album directories")
                sys.stdout.flush()

                return full_export_dir
            else:
                raise ValueError("No albums data found")

        except Exception as e:
            error_msg = f"Error creating album structure: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            raise

    def create_date_structure(self, date_format: str) -> Path:
        """Create the date-based folder structure and return working directory"""
        try:
            base_dir = self.output_dir / "full_library_export"
            working_dir = base_dir / "by_date"

            print(f"Creating date structure in: {working_dir}")
            sys.stdout.flush()

            # Create base directories
            for dir_path in [self.output_dir, base_dir, working_dir]:
                dir_path.mkdir(parents=True, exist_ok=True)
                if not dir_path.exists():
                    raise ValueError(f"Failed to create directory: {dir_path}")

            return working_dir

        except Exception as e:
            error_msg = f"Error creating date structure: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            raise

    def process_photos(self, organization: str, date_format: str = None):
        """Process photos with enhanced debugging"""
        self.unique_total = len(set(self.photo_to_albums.keys()))
        self.start_time = time.time()

        print(f"\nProcessing {self.unique_total} photos")
        sys.stdout.flush()

        if organization == 'by_date':
            working_dir = self.create_date_structure(date_format)
        else:
            working_dir = self.create_album_structure()

        photo_items = list(self.photo_to_albums.items())
        total_processed = 0
        exif_writer = ExifWriter(self.flickr_wrapper, self.photo_to_albums, self.account_data, self.include_extended_description, self.write_xmp_sidecars)
        processor = ProcessSinglePhoto(self.photo_id_map, exif_writer, self.flickr_export_metadata, self.failed_dir, date_format, self.write_xmp_sidecars)

        for batch_idx, batch_start in enumerate(range(0, len(photo_items), self.batch_size), 1):
            batch_end = min(batch_start + self.batch_size, len(photo_items))
            current_batch = photo_items[batch_start:batch_end]

            with ThreadPoolExecutor(max_workers=self.cpu_cores) as executor:
                futures = []
                for photo_id, albums in current_batch:
                    if organization == 'by_date':
                        future = executor.submit(
                            processor._process_single_photo_by_date,
                            photo_id, working_dir
                        )
                    else:
                        future = executor.submit(
                            processor._process_single_photo,
                            photo_id, albums, working_dir
                        )
                    futures.append((future, photo_id))

                # Process results for this batch
                for future, photo_id in futures:
                    result = future.result()
                    if result and isinstance(result, tuple):
                        source_file, dest_file = result
                        if dest_file and dest_file.exists():
                            self.unique_successful.add(photo_id)
                        else:
                            self.unique_failed.add(photo_id)
                    else:
                        self.unique_failed.add(photo_id)

                    total_processed += 1

                # Update progress after processing batch
                elapsed_time = time.time() - self.start_time
                photos_per_second = total_processed / elapsed_time if elapsed_time > 0 else 0
                remaining_photos = self.unique_total - total_processed
                estimated_remaining = remaining_photos / photos_per_second if photos_per_second > 0 else 0

                elapsed = time.strftime('%H:%M:%S', time.gmtime(elapsed_time))
                remaining = time.strftime('%H:%M:%S', time.gmtime(estimated_remaining))

                # Show current batch info and overall progress
                if organization == 'by_date':
                    current_location = f"Processing date structure - Batch {batch_idx}"
                else:
                    current_location = f"Processing albums - Batch {batch_idx}"

                print(f"\r{current_location} | Completed {total_processed} of {self.unique_total}")
                print(f"\rTotal: {total_processed}/{self.unique_total} files | "
                    f"Success: {len(self.unique_successful)} | "
                    f"Failed: {len(self.unique_failed)} | "
                    f"Time elapsed: {elapsed} | "
                    f"Estimated remaining: {remaining}", end='\r')
                sys.stdout.flush()

            self._cleanup_memory()

        print("\n\nProcessing Complete")
        sys.stdout.flush()

    def _cleanup_memory(self):
        """Perform memory cleanup between batches"""
        gc.collect()

        # Log memory usage
        #process = psutil.Process()
       # memory_info = process.memory_info()
       # logging.debug(f"Memory usage after cleanup: {memory_info.rss / (1024*1024):.1f}MB")

    def _check_memory_pressure(self) -> bool:
        """Check if memory usage exceeds threshold"""
        try:
            memory_info = psutil.Process().memory_info()
            system_memory = psutil.virtual_memory()

            memory_usage = memory_info.rss
            memory_limit = self.max_memory

            logging.debug(f"Memory usage: {memory_usage / (1024*1024):.1f}MB")
            logging.debug(f"Memory limit: {memory_limit / (1024*1024):.1f}MB")

            return memory_usage > memory_limit

        except Exception as e:
            logging.warning(f"Error checking memory: {str(e)}")
            return False

    def _update_progress(self, processed: int, total: int):
        """Update progress with current statistics"""
        percentage = (processed / total) * 100
        progress_msg = (
            f"\rProgress: {processed}/{total} ({percentage:.1f}%) - "
            f"Success: {self.stats['successful']['count']}, "
            f"Failed: {self.stats['failed']['count']}"
        )
        print(progress_msg, end='', flush=True)
        logging.info(progress_msg.replace('\r', ''))

    def _save_checkpoint(self, processed: int, batch_end: int):
        """Save processing checkpoint"""
        try:
            checkpoint = {
                'processed': processed,
                'batch_end': batch_end,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }

            checkpoint_file = self.results_dir / 'processing_checkpoint.json'
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)

            logging.debug(f"Saved checkpoint at {processed} photos")

        except Exception as e:
            logging.warning(f"Failed to save checkpoint: {str(e)}")

    def _print_final_stats(self, processed: int, total: int):
        """Print final processing statistics"""
        print("\nProcessing complete!")
        print(f"Processed {processed}/{total} photos")
        print(f"Successful: {self.stats['successful']['count']}")
        print(f"Failed: {self.stats['failed']['count']}")

        memory_info = psutil.Process().memory_info()
        logging.info(f"Final memory usage: {memory_info.rss / (1024*1024):.1f}MB")



    def _find_photo_file(self, photo_id: str, filename: str) -> Optional[Path]:
        """Find the original photo file using the Flickr ID with enhanced logging"""
        logging.debug(f"Searching for photo file {photo_id} (filename: {filename})")
        logging.debug(f"Looking in directory: {self.photos_dir}")


        # Normalize photo_id and filename for case-insensitive comparison
        photo_id = photo_id.lower()  # Make photo ID lowercase
        normalized_filename = filename.lower()  # Make filename lowercase

        # First try: exact match with photo ID (case insensitive)
        matches = []
        for file in self.photos_dir.iterdir():
            if f"_{photo_id}_" in file.name.lower() or f"_{photo_id}." in file.name.lower():
                matches.append(file)
                logging.debug(f"Found exact match: {file.name}")

        if matches:
            if len(matches) > 1:
                logging.warning(f"Multiple matches found for {photo_id}, using first one: {matches[0]}")
            return matches[0]

        # Second try: normalize ID and try again (case insensitive)
        normalized_id = photo_id.lstrip('0')  # Remove leading zeros
        logging.debug(f"Trying normalized ID: {normalized_id}")

        for file in self.photos_dir.iterdir():
            file_parts = file.name.lower().split('_')  # Make lowercase for comparison
            for part in file_parts:
                clean_part = part.split('.')[0]  # Remove extension if present
                if clean_part.lstrip('0') == normalized_id:
                    logging.debug(f"Found match with normalized ID: {file.name}")
                    return file

        # Third try: look for filename (case insensitive)
        for file in self.photos_dir.iterdir():
            if file.name.lower().startswith(normalized_filename):
                logging.debug(f"Found match by filename: {file.name}")
                return file

        # If we get here, we couldn't find the file
        logging.debug("Listing sample of files in photos directory:")
        all_files = list(self.photos_dir.iterdir())
        logging.debug(f"Total files in directory: {len(all_files)}")
        if all_files:
            for f in all_files[:5]:
                logging.debug(f"  - {f.name} (lowercase: {f.name.lower()})")

        logging.error(f"Could not find media file for {photo_id} ({filename})")
        return None


    def _build_gps_xmp(self, geo: Dict) -> str:
        """Build GPS XMP tags if geo data is available"""
        if not geo or 'latitude' not in geo or 'longitude' not in geo:
            return ""

        return f"""
        <exif:GPSLatitude>{geo['latitude']}</exif:GPSLatitude>
        <exif:GPSLongitude>{geo['longitude']}</exif:GPSLongitude>"""

    def print_statistics(self):
        """Print processing statistics"""
        # Create a console handler that will show these stats regardless of quiet flag
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(formatter)

        # Store the original handlers
        original_handlers = logging.getLogger().handlers
        logging.getLogger().handlers = [console_handler]

        try:
            # Print main statistics
            logging.info("\nProcessing Statistics:")
            logging.info("-" * 20)
            logging.info(f"Total unique photos: {self.unique_total}")
            logging.info(f"Successfully processed: {len(self.unique_successful)}")
            logging.info(f"Failed: {len(self.unique_failed)}")
            logging.info(f"Skipped: {self.stats['skipped']['count']}")
            logging.info("-" * 20)

            # Show failure details if there are any failures
            if len(self.unique_failed) > 0:
                # Restore original handlers for error reporting
                logging.getLogger().handlers = original_handlers

                logging.error("\nFailure Details:")
                # Show metadata failures
                if self.stats['failed']['metadata']['details']:
                    logging.error("Metadata failures:")
                    for file, error, exported in self.stats['failed']['metadata']['details'][:3]:
                        logging.error(f"- {file}: {error} (File exported: {'Yes' if exported else 'No'})")
                    if len(self.stats['failed']['metadata']['details']) > 3:
                        logging.error(f"... and {len(self.stats['failed']['metadata']['details']) - 3} more metadata failures")

                # Show file copy failures
                if self.stats['failed']['file_copy']['details']:
                    logging.error("File copy failures:")
                    for file, error in self.stats['failed']['file_copy']['details'][:3]:
                        logging.error(f"- {file}: {error}")
                    if len(self.stats['failed']['file_copy']['details']) > 3:
                        logging.error(f"... and {len(self.stats['failed']['file_copy']['details']) - 3} more file copy failures")

                logging.error("\nSee processing_results.txt for complete details")

        finally:
            # Restore original handlers
            logging.getLogger().handlers = original_handlers


