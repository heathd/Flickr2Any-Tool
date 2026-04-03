
import logging
import subprocess
from pathlib import Path
from typing import Dict, List

from PIL import ExifTags, Image

from flickr_to_anytool.constants import EXIF_ORIENTATION_MAP
from flickr_to_anytool.flickr_api_metadata import FlickrApiMetadata
from flickr_to_anytool.jpeg_verifier import JPEGVerifier



class ExifWriter:
    def __init__(self, flickr_wrapper: FlickrApiMetadata, photo_to_albums, account_data, include_extended_description: bool, write_xmp_sidecars: bool):
        self.flickr_wrapper = flickr_wrapper
        self.photo_to_albums = photo_to_albums
        self.account_data = account_data
        self.include_extended_description = include_extended_description
        self.write_xmp_sidecars = write_xmp_sidecars

    def _embed_image_metadata(self, photo_file: Path, metadata: Dict):
        """Embed metadata into an image file using exiftool"""
        try:
            # First verify JPEG integrity if it's a JPEG file
            if photo_file.suffix.lower() in ['.jpg', '.jpeg']:
                is_valid, error_msg = JPEGVerifier.is_jpeg_valid(str(photo_file))
                if not is_valid:
                    if not JPEGVerifier.attempt_repair(str(photo_file)):
                        raise ValueError(f"Invalid JPEG file: {error_msg}")

            args = self._build_exiftool_args(photo_file, metadata)

            # Run exiftool
            result = subprocess.run(args, capture_output=True, text=True)

            if result.returncode != 0:
                # If exiftool fails, try again with strip_orientation=True to remove broken orientation
                logging.debug(f"First exiftool attempt failed, retrying with orientation stripping")
                args = self._build_exiftool_args(photo_file, metadata, strip_orientation=True)
                result = subprocess.run(args, capture_output=True, text=True)

                if result.returncode != 0:
                    raise ValueError(f"Exiftool error: {result.stderr}")

            if result.stderr and 'error' in result.stderr.lower():
                logging.warning(f"Exiftool warning for {photo_file}: {result.stderr}")

        except Exception as e:
            error_msg = f"Error embedding metadata in {photo_file}: {str(e)}"
            logging.error(error_msg)
            raise

    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.tiff', '.tif', '.JPG', '.JPEG', '.TIFF', '.TIF'}

    def _build_base_args(self, metadata: Dict) -> List[str]:
        """Core exiftool flags and metadata fields common to all files."""
        enhanced_description = self._build_formatted_description(metadata)
        return [
            'exiftool',
            '-overwrite_original',
            '-ignoreMinorErrors',
            '-m',
            f'-DateTimeOriginal={metadata["date_taken"]}',
            f'-CreateDate={metadata["date_taken"]}',
            f'-Title={metadata.get("name", "")}',
            f'-ImageDescription={enhanced_description}',
            f'-IPTC:Caption-Abstract={enhanced_description}',
            f'-Copyright={metadata.get("license", "All Rights Reserved")}',
            f'-Artist={self.account_data.get("real_name", "")}',
            f'-Creator={self.account_data.get("real_name", "")}',
            *[f'-Keywords={tag["tag"]}' for tag in metadata.get('tags', [])],
        ]

    def _compute_orientation(self, media_file: Path, metadata: Dict) -> int:
        """
        Read current EXIF orientation and apply Flickr rotation metadata.
        Returns a valid EXIF orientation value (1-8).
        Raises if the image cannot be opened.
        """
        rotation_to_orientation = {90: 6, 180: 3, 270: 8}
        rotation_to_final = {0: 1, 90: 6, 180: 3, 270: 8}

        with Image.open(media_file) as img:
            exif = img._getexif()
            if not exif:
                return 1

            orientation_tag = next(
                (tag_id for tag_id in ExifTags.TAGS if ExifTags.TAGS[tag_id] == 'Orientation'),
                None
            )
            current_orientation = exif.get(orientation_tag)

            logging.debug(f"Processing orientation for {media_file}: current={current_orientation}, "
                          f"dimensions={img.size}")

            if current_orientation is None:
                return 1

            if 'rotation' in metadata:
                rotation_degrees = int(metadata["rotation"])
                logging.debug(f"Flickr rotation value: {rotation_degrees}")
                new_orientation = rotation_to_orientation.get(rotation_degrees, current_orientation) \
                    if rotation_degrees > 0 else current_orientation
            else:
                new_orientation = current_orientation

            if current_orientation and new_orientation != current_orientation:
                current_rotation = EXIF_ORIENTATION_MAP.get(current_orientation, {}).get('rotation', 0)
                new_rotation = EXIF_ORIENTATION_MAP.get(new_orientation, {}).get('rotation', 0)
                new_orientation = rotation_to_final.get((current_rotation + new_rotation) % 360, 1)

            if current_orientation != new_orientation:
                old_desc = EXIF_ORIENTATION_MAP.get(current_orientation, {}).get('description', 'Unknown')
                new_desc = EXIF_ORIENTATION_MAP.get(new_orientation, {}).get('description', 'Unknown')
                logging.debug(f"Changing orientation: {current_orientation} ({old_desc}) -> "
                              f"{new_orientation} ({new_desc})")

            return new_orientation

    def _build_orientation_args(self, media_file: Path, metadata: Dict, strip_orientation: bool) -> List[str]:
        """
        Returns orientation-related exiftool args for image files.
        Uses strip_orientation=True to clear corrupt EXIF and set a safe default.
        """
        if strip_orientation:
            logging.debug(f"Stripping corrupt EXIF from {media_file}, using defaults")
            return [
                '-IFD0:*=',
                '-IFD0:Orientation#=1',
            ]

        try:
            new_orientation = self._compute_orientation(media_file, metadata)
            args = [
                f'-IFD0:Orientation#={new_orientation}',
                '-IFD0:YCbCrPositioning=1',
                '-IFD0:YCbCrSubSampling=2 2',
            ]
            if EXIF_ORIENTATION_MAP.get(new_orientation, {}).get('mirrored', False):
                args.append('-Flop')
            return args
        except Exception as e:
            logging.warning(f"Error handling image orientation for {media_file}: {str(e)}")
            logging.debug("Exception details:", exc_info=True)
            return []

    def _build_gps_args(self, metadata: Dict) -> List[str]:
        """Returns GPS exiftool args if geo data is present."""
        geo = metadata.get('geo', {})
        if 'latitude' in geo and 'longitude' in geo:
            return [
                f'-GPSLatitude={geo["latitude"]}',
                f'-GPSLongitude={geo["longitude"]}',
            ]
        return []

    def _build_exiftool_args(self, media_file: Path, metadata: Dict, is_video: bool = False, strip_orientation: bool = False) -> List[str]:
        """Build exiftool arguments for metadata embedding."""
        args = self._build_base_args(metadata)

        if media_file.suffix.lower() in self.IMAGE_EXTENSIONS:
            args += self._build_orientation_args(media_file, metadata, strip_orientation)

        args += self._build_gps_args(metadata)
        args.append(str(media_file))
        return args


    def _build_formatted_description(self, metadata: Dict) -> str:
        """Create a formatted description including key metadata fields based on configuration"""
        if not self.include_extended_description:
            return metadata.get("description", "")

        metadata_sections = []

        # Add highlight rank if available (always at the top)
        if 'engagement' in metadata and 'rank' in metadata['engagement'] and 'total_ranked' in metadata['engagement']:
            metadata_sections.extend([
                f"Highlight Rank: #{metadata['engagement']['rank']} (of {metadata['engagement']['total_ranked']})",
                "-----"  # Separator after rank
            ])

        # Add description if it exists and isn't empty
        if metadata.get("description"):
            metadata_sections.extend([
                "Description:",
                metadata.get("description")
            ])

        # Add comments section if there are any comments
        if metadata.get('comments'):
            metadata_sections.extend([
                "",
                "Flickr Comments:",
                *[f"- {self._format_user_comment(comment)}"
                for comment in metadata.get('comments', [])]
            ])

        # Add favorites section if API is connected and photo has favorites
        try:
            fave_count = int(metadata.get('count_faves', '0'))
        except (ValueError, TypeError):
            fave_count = 0

        if self.flickr_wrapper and fave_count > 0:
            favorites = self.flickr_wrapper._get_photo_favorites(metadata['id'])
            if favorites:
                metadata_sections.extend([
                    "",
                    "Flickr Faves:",
                    *[f"- {fave['username'] or fave['nsid']} ({fave['favedate']})"
                    for fave in favorites]
                ])

        # Add albums section
        if photo_id := metadata.get('id'):
            albums = self.photo_to_albums.get(photo_id, [])
            if albums:
                metadata_sections.extend([
                    "",
                    "Flickr Albums:",
                    *[f"- {album_name}" for album_name in albums]
                ])

        # Add the rest of metadata sections
        metadata_sections.extend([
            "",
            "-----",
            "Flickr Meta:",
            f"View Count: {metadata.get('count_views', '0')}",
            f"Favorite Count: {metadata.get('count_faves', '0')}",
            f"Comment Count: {metadata.get('count_comments', '0')}",
            "--",
            f"Privacy: {metadata.get('privacy', '')}",
            f"Safety Level: {metadata.get('safety', '')}",
            "--",
            f"Flickr URL: {metadata.get('photopage', '')}",
            f"Creator Profile: {self.account_data.get('screen_name', '')} / {self.account_data.get('profile_url', '')}",
            "--",
        ])

        # Filter out empty sections and join with newlines
        description = "\n".join(section for section in metadata_sections if section is not None and section != "")
        return description


    def _format_user_comment(self, comment) -> str:
        """Format a user comment with username and realname if available"""
        if self.flickr_wrapper:
            username, realname = self.flickr_wrapper._get_user_info(comment['user'])
        else:
            username, realname = comment['user'], ""
        if realname:
            user_display = f"{realname} ({username})"
        else:
            user_display = username
        return f"{user_display} ({comment['date']}): {comment['comment']}"


    def _embed_video_metadata(self, video_file: Path, metadata: Dict):
        """Embed metadata into a video file using exiftool"""
        try:
            # Build video-specific exiftool arguments
            args = self._build_exiftool_args(video_file, metadata, is_video=True)

            # Run exiftool
            result = subprocess.run(args, capture_output=True, text=True)

            if result.returncode != 0:
                raise ValueError(f"Exiftool error: {result.stderr}")

            if result.stderr and 'error' in result.stderr.lower():
                logging.warning(f"Exiftool warning for {video_file}: {result.stderr}")

        except Exception as e:
            error_msg = f"Error embedding metadata in {video_file}: {str(e)}"
            logging.error(error_msg)
            raise


    def _write_xmp_sidecar(self, media_file: Path, metadata: Dict):
        """Create XMP sidecar file with extended and Flickr-specific metadata"""
        sidecar_file = Path(str(media_file) + '.xmp')

        # Get enhanced description using existing method
        enhanced_description = self._build_formatted_description(metadata)

        # Build tag list from Flickr tags
        tags = [tag["tag"] for tag in metadata.get('tags', [])]

        # Function to safely encode text for XML
        def xml_escape(text):
            if not isinstance(text, str):
                text = str(text)
            return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&apos;')

        xmp_content = f"""<?xpacket begin="" id="W5M0MpCehiHzreSzNTczkc9d"?>
        <x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="XMP Core 5.1.2">
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:photoshop="http://ns.adobe.com/photoshop/1.0/"
                xmlns:xmp="http://ns.adobe.com/xap/1.0/"
                xmlns:lr="http://ns.adobe.com/lightroom/1.0/"
                xmlns:flickr="http://flickr.com/schema/2024/01/">
        <rdf:Description rdf:about="">
            <!-- Enhanced Description -->
            <dc:description>
                <rdf:Alt>
                    <rdf:li xml:lang="x-default">{xml_escape(enhanced_description)}</rdf:li>
                </rdf:Alt>
            </dc:description>
            <!-- Engagement Metrics -->
                    <flickr:engagement rdf:parseType="Resource">
                        <flickr:rank>{xml_escape(str(metadata.get('engagement', {}).get('rank', '0')))}</flickr:rank>
                        <flickr:totalRanked>{xml_escape(str(metadata.get('engagement', {}).get('total_ranked', '0')))}</flickr:totalRanked>
                        <flickr:favoriteCount>{xml_escape(str(metadata.get('engagement', {}).get('favorites', '0')))}</flickr:favoriteCount>
                        <flickr:commentCount>{xml_escape(str(metadata.get('engagement', {}).get('comments', '0')))}</flickr:commentCount>
                    </flickr:engagement>
            <!-- Tags -->
            <dc:subject>
                <rdf:Bag>
                    {''.join(f'<rdf:li>{xml_escape(tag)}</rdf:li>' for tag in tags)}
                </rdf:Bag>
            </dc:subject>

            <!-- Basic Metadata -->
            <dc:title>
                <rdf:Alt>
                    <rdf:li xml:lang="x-default">{xml_escape(metadata.get("name", ""))}</rdf:li>
                </rdf:Alt>
            </dc:title>
            <dc:creator>
                <rdf:Seq>
                    <rdf:li>{xml_escape(self.account_data.get("real_name", ""))}</rdf:li>
                </rdf:Seq>
            </dc:creator>
            <dc:rights>
                <rdf:Alt>
                    <rdf:li xml:lang="x-default">{xml_escape(metadata.get("license", "All Rights Reserved"))}</rdf:li>
                </rdf:Alt>
            </dc:rights>
            <xmp:CreateDate>{xml_escape(metadata["date_taken"])}</xmp:CreateDate>
            <xmp:ModifyDate>{xml_escape(metadata["date_taken"])}</xmp:ModifyDate>

            <!-- Photo-specific Flickr metadata -->
            <flickr:id>{xml_escape(metadata["id"])}</flickr:id>
            <flickr:photopage>{xml_escape(metadata["photopage"])}</flickr:photopage>
            <flickr:original>{xml_escape(metadata["original"])}</flickr:original>
            <flickr:viewCount>{xml_escape(metadata.get("count_views", "0"))}</flickr:viewCount>
            <flickr:favoriteCount>{xml_escape(metadata.get("count_faves", "0"))}</flickr:favoriteCount>
            <flickr:commentCount>{xml_escape(metadata.get("count_comments", "0"))}</flickr:commentCount>
            <flickr:tagCount>{xml_escape(metadata.get("count_tags", "0"))}</flickr:tagCount>
            <flickr:noteCount>{xml_escape(metadata.get("count_notes", "0"))}</flickr:noteCount>

            <!-- Privacy and Permissions -->
            <flickr:privacy>{xml_escape(metadata.get("privacy", ""))}</flickr:privacy>
            <flickr:commentPermissions>{xml_escape(metadata.get("comment_permissions", ""))}</flickr:commentPermissions>
            <flickr:taggingPermissions>{xml_escape(metadata.get("tagging_permissions", ""))}</flickr:taggingPermissions>
            <flickr:safety>{xml_escape(metadata.get("safety", ""))}</flickr:safety>

            <!-- Account Information -->
            <flickr:accountInfo rdf:parseType="Resource">
                <flickr:realName>{xml_escape(self.account_data.get("real_name", ""))}</flickr:realName>
                <flickr:screenName>{xml_escape(self.account_data.get("screen_name", ""))}</flickr:screenName>
                <flickr:joinDate>{xml_escape(self.account_data.get("join_date", ""))}</flickr:joinDate>
                <flickr:profileUrl>{xml_escape(self.account_data.get("profile_url", ""))}</flickr:profileUrl>
                <flickr:nsid>{xml_escape(self.account_data.get("nsid", ""))}</flickr:nsid>
                <flickr:proUser>{xml_escape(self.account_data.get("pro_user", "no"))}</flickr:proUser>
            </flickr:accountInfo>

            <!-- Comments -->
            <flickr:comments>
                <rdf:Bag>
                    {''.join(f'''<rdf:li rdf:parseType="Resource">
                    <flickr:commentId>{xml_escape(comment["id"])}</flickr:commentId>
                    <flickr:commentDate>{xml_escape(comment["date"])}</flickr:commentDate>
                    <flickr:commentUser>{xml_escape(comment["user"])}</flickr:commentUser>
                    <flickr:commentText>{xml_escape(comment["comment"])}</flickr:commentText>
                    </rdf:li>''' for comment in metadata.get("comments", []))}
                </rdf:Bag>
            </flickr:comments>

            <!-- Favorites -->
            <flickr:favorites>
                <rdf:Bag>
                    {''.join(f'''<rdf:li rdf:parseType="Resource">
                    <flickr:favoriteUser>{xml_escape(fave["username"] or fave["nsid"])}</flickr:favoriteUser>
                    <flickr:favoriteDate>{xml_escape(fave["favedate"])}</flickr:favoriteDate>
                    </rdf:li>''' for fave in (self.flickr_wrapper._get_photo_favorites(metadata['id']) if self.flickr_wrapper and int(metadata.get('count_faves', '0')) > 0 else []))}
                </rdf:Bag>
            </flickr:favorites>

            <!-- Albums -->
            <flickr:albums>
                <rdf:Bag>
                    {''.join(f'''<rdf:li>{xml_escape(album_name)}</rdf:li>'''
                    for album_name in self.photo_to_albums.get(metadata.get('id', ''), []))}
                </rdf:Bag>
            </flickr:albums>

        </rdf:Description>
        </rdf:RDF>
        </x:xmpmeta>
        <?xpacket end="w"?>"""

        with open(sidecar_file, 'w', encoding='utf-8') as f:
            f.write(xmp_content)
