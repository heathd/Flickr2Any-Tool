

from typing import Dict

import json
import logging
from pathlib import Path
import shutil
import sys
import traceback
from typing import List

from flickr_to_anytool.constants import MediaType
from flickr_to_anytool.exif_writer import ExifWriter
from flickr_to_anytool.flickr_export_metadata import FlickrExportMetadata
from flickr_to_anytool.interestingness_filter import InterestingnessFilter
from flickr_to_anytool.output_helpers import OutputHelpers



class InterestingAlbumCreator:
    def __init__(self, account_data, resume: bool, exif_writer: ExifWriter, photo_id_map: Dict, interestingness_filter: InterestingnessFilter, flickr_export_metadata: FlickrExportMetadata, output_dir: Path, stats: Dict, write_xmp_sidecars: bool):
        self.account_data = account_data
        self.resume = resume
        self.exif_writer = exif_writer
        self.photo_id_map = photo_id_map
        self.interestingness_filter = interestingness_filter
        self.flickr_export_metadata = flickr_export_metadata
        self.output_dir = output_dir
        self.stats = stats
        self.write_xmp_sidecars = write_xmp_sidecars

    def create_interesting_albums(self, time_period: str, photo_count: int = 100):
        """Create albums of user's most engaging photos"""
        try:
            # Create highlights_only parent directory
            highlights_dir = self.output_dir / "highlights_only"
            print(f"\nCreating highlights directory: {highlights_dir}")
            sys.stdout.flush()

            highlights_dir.mkdir(parents=True, exist_ok=True)

            # Get photos with engagement metrics
            logging.info("\nAnalyzing photos for engagement metrics...")
            sys.stdout.flush()

            all_photos = self._fetch_user_interesting_photos(time_period, photo_count)
            if not all_photos:
                logging.info("No photos found meeting engagement criteria")
                return

            logging.info(f"\nFound {len(all_photos)} photos to process")
            logging.info("Creating highlight albums...")
            sys.stdout.flush()

            # Initialize privacy groups
            privacy_groups = {
                'private': [],
                'friends & family': [],
                'friends only': [],
                'family only': [],
                'public': []
            }

            # Process each photo
            for photo in all_photos:
                # Get privacy value and normalize it
                raw_privacy = photo.get('privacy', '').lower().strip()

                # Map privacy values
                privacy_mapping = {
                    'private': 'private',
                    'friend & family': 'friends & family',
                    'friends & family': 'friends & family',
                    'friends&family': 'friends & family',
                    'friendandfamily': 'friends & family',
                    'friend and family': 'friends & family',
                    'friends and family': 'friends & family',
                    'friends': 'friends only',
                    'friend': 'friends only',
                    'family': 'family only',
                    'public': 'public',
                    '': 'private'
                }

                normalized_privacy = privacy_mapping.get(raw_privacy, 'private')
                privacy_groups[normalized_privacy].append(photo)

            # Create albums for each privacy group
            total_exported = 0
            for privacy_type, photos in privacy_groups.items():
                if photos:
                    print(f"\nProcessing {len(photos)} {privacy_type} photos...")
                    sys.stdout.flush()

                    folder_name = f"0{list(privacy_groups.keys()).index(privacy_type) + 1}_{privacy_type.replace(' ', '_').title()}_Highlights"

                    self._create_single_interesting_album(
                        highlights_dir,
                        folder_name,
                        f"Your most engaging {privacy_type} Flickr photos",
                        photos
                    )
                    total_exported += len(photos)

            print(f"\nHighlight albums creation complete!")
            print(f"Total photos exported: {total_exported}")
            sys.stdout.flush()

        except Exception as e:
            error_msg = f"Error creating highlight albums: {str(e)}"
            print(error_msg)
            print(traceback.format_exc())
            logging.error(error_msg)
            raise

    def _fetch_user_interesting_photos(self, time_period: str, per_page: int = 100) -> List[Dict]:
        """Process user's photos and sort by engagement metrics"""
        try:
            logging.info("\nAnalyzing photos for engagement metrics...")

            # Use pre-loaded metadata cache instead of re-reading files
            interesting_photos = []

            # Create temporary file for storing results
            temp_results_file = self.output_dir / "temp_highlights.json"

            # If resuming and temp file exists, load previous results
            if self.resume and temp_results_file.exists():
                try:
                    with open(temp_results_file, 'r') as f:
                        return json.load(f)
                except Exception:
                    logging.warning("Failed to load previous results from temp file. Continuing with normal processing.")

            total_files = len(self.photo_id_map)
            processed = 0
            meeting_criteria = 0

            logging.info(f"Processing {total_files} photos")
            logging.info("Parameters:")
            logging.info(f"- Views: min {self.interestingness_filter['min_views']} (weight: {self.interestingness_filter['view_weight']})")
            logging.info(f"- Favorites: min {self.interestingness_filter['min_faves']} (weight: {self.interestingness_filter['fave_weight']})")
            logging.info(f"- Comments: min {self.interestingness_filter['min_comments']} (weight: {self.interestingness_filter['comment_weight']})")

            # Process metadata from cache
            for photo_id in self.photo_id_map:
                processed += 1

                # Load metadata for each photo
                photo_metadata = self.flickr_export_metadata.get(photo_id)
                if not photo_metadata:
                    continue

                if processed % 1000 == 0:
                    print(f"\rAnalyzing: {processed}/{total_files} ({(processed/total_files)*100:.1f}%) - "
                        f"Found {meeting_criteria} matching photos", end='')
                    sys.stdout.flush()

                try:
                    # Get metrics with default values
                    faves = int(photo_metadata.get('count_faves', 0))
                    comments = int(photo_metadata.get('count_comments', 0))
                    views = int(photo_metadata.get('count_views', 0))

                    # Check if meets criteria
                    if (views >= self.interestingness_filter['min_views'] or
                        faves >= self.interestingness_filter['min_faves'] or
                        comments >= self.interestingness_filter['min_comments']):

                        # Calculate score
                        interestingness_score = (
                            (faves * self.interestingness_filter['fave_weight']) +
                            (comments * self.interestingness_filter['comment_weight']) +
                            (views * self.interestingness_filter['view_weight'])
                        )

                        # Get source file from pre-loaded map
                        source_file = self.photo_id_map.get(photo_id)
                        if not source_file:
                            continue

                        photo_data = {
                            'id': photo_id,
                            'title': photo_metadata.get('name', ''),
                            'description': photo_metadata.get('description', ''),
                            'date_taken': photo_metadata.get('date_taken', ''),
                            'license': photo_metadata.get('license', ''),
                            'fave_count': faves,
                            'comment_count': comments,
                            'count_views': views,
                            'interestingness_score': interestingness_score,
                            'original_file': str(source_file),
                            'original': str(source_file),
                            'privacy': photo_metadata.get('privacy', ''),
                            'safety': photo_metadata.get('safety', '')
                        }
                        interesting_photos.append(photo_data)
                        meeting_criteria += 1

                except Exception as e:
                    continue

            print(f"\n\nFound {meeting_criteria} photos meeting criteria")

            if interesting_photos:
                # Sort by score
                interesting_photos.sort(key=lambda x: x['interestingness_score'], reverse=True)
                interesting_photos = interesting_photos[:per_page]

                # Save results to temp file
                try:
                    with open(temp_results_file, 'w') as f:
                        json.dump(interesting_photos, f)
                except Exception:
                    pass  # Continue even if saving fails

                return interesting_photos

            return []

        finally:
            # Cleanup
            if 'temp_results_file' in locals() and temp_results_file.exists():
                try:
                    temp_results_file.unlink()
                except Exception:
                    pass

    def _create_single_interesting_album(self, highlights_dir: Path, folder_name: str, description: str, photos: List[Dict]):
        """Create a single album of engaging photos"""
        try:
            # Create folder for this privacy level
            album_dir = highlights_dir / folder_name
            album_dir.mkdir(parents=True, exist_ok=True)

            total_photos = len(photos)
            print(f"Processing {total_photos} photos for {folder_name}")
            sys.stdout.flush()

            # Update total files stat
            self.stats['total_files'] += total_photos

            processed = 0
            for i, photo in enumerate(photos, 1):
                source_file = photo['original_file']
                if not isinstance(source_file, Path):
                    source_file = Path(source_file)

                if not source_file.exists():
                    self.stats['skipped']['count'] += 1
                    self.stats['skipped']['details'].append(
                        (str(source_file), "Source file not found")
                    )
                    continue

                # Create filename without Flickr ID
                if photo['title']:
                    safe_title = OutputHelpers.sanitize_folder_name(photo['title'])
                    photo_filename = f"{safe_title}{source_file.suffix}"
                else:
                    photo_filename = OutputHelpers.get_destination_filename(photo['id'], source_file, photo)

                dest_file = album_dir / photo_filename

                # Handle filename conflicts
                counter = 1
                base_name = dest_file.stem
                extension = dest_file.suffix
                while dest_file.exists():
                    dest_file = album_dir / f"{base_name}_{counter}{extension}"
                    counter += 1

                if i % 5 == 0 or i == total_photos:
                    logging.info(f"\r{folder_name}: {i}/{total_photos} ({(i/total_photos)*100:.1f}%)")
                    sys.stdout.flush()

                # Copy file
                shutil.copy2(source_file, dest_file)

                # Prepare metadata
                photo_metadata = photo.copy()
                photo_metadata['original_file'] = str(source_file)
                photo_metadata['original'] = str(source_file)

                # Add engagement metrics to metadata
                photo_metadata['engagement'] = {
                    'rank': i,
                    'total_ranked': total_photos,
                    'favorites': photo['fave_count'],
                    'comments': photo['comment_count'],
                    'views': photo.get('count_views', 0)
                }

                # Ensure photopage exists
                if 'photopage' not in photo_metadata:
                    photo_metadata['photopage'] = f"https://www.flickr.com/photos/{self.account_data.get('nsid', '')}/{photo['id']}"

                # Embed metadata based on media type
                media_type = OutputHelpers.get_media_type(dest_file)
                if media_type == MediaType.IMAGE:
                    self.exif_writer._embed_image_metadata(dest_file, photo_metadata)
                elif media_type == MediaType.VIDEO:
                    self.exif_writer._embed_video_metadata(dest_file, photo_metadata)

                if self.write_xmp_sidecars:
                    self.exif_writer._write_xmp_sidecar(dest_file, photo_metadata)

                processed += 1
                self.stats['successful']['count'] += 1
                self.stats['successful']['details'].append(
                    (str(source_file), str(dest_file), "Highlight photo processed successfully")
                )


            print(f"\nCompleted {folder_name}: {processed}/{total_photos} photos processed successfully")
            sys.stdout.flush()

        except Exception as e:
            error_msg = f"Error creating album {folder_name}: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            raise
