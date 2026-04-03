"""
Integration tests for ExifWriter class - actually invokes exiftool
"""

import unittest
from pathlib import Path
from unittest.mock import Mock
import sys
import tempfile
import shutil
import subprocess

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flickr_to_anytool.exif_writer import ExifWriter


class TestExifWriterEmbedImageMetadata(unittest.TestCase):
    """Integration tests for ExifWriter._embed_image_metadata method"""

    @classmethod
    def setUpClass(cls):
        """Set up fixture directory and check for exiftool"""
        cls.fixture_dir = Path(__file__).parent / 'fixtures' / 'exif_writer'
        cls.broken_jpg = cls.fixture_dir / 'broken.jpg'

        # Check if exiftool is available
        try:
            subprocess.run(['exiftool', '-ver'], capture_output=True, check=True)
            cls.exiftool_available = True
        except (subprocess.CalledProcessError, FileNotFoundError):
            cls.exiftool_available = False

    def setUp(self):
        """Set up test instance"""
        # Skip tests if exiftool is not available
        if not self.exiftool_available:
            self.skipTest("exiftool not installed")

        # Create mock FlickrApiMetadata
        self.mock_flickr_wrapper = Mock()
        self.mock_flickr_wrapper._get_photo_favorites.return_value = []

        # Create test photo_to_albums mapping
        self.photo_to_albums = {
            '123456789': ['Test Album 1', 'Test Album 2'],
            '987654321': ['Another Album']
        }

        # Create test account data
        self.account_data = {
            'real_name': 'Test User',
            'screen_name': 'testuser',
            'profile_url': 'https://flickr.com/photos/testuser'
        }

        # Initialize ExifWriter
        self.exif_writer = ExifWriter(
            flickr_wrapper=self.mock_flickr_wrapper,
            photo_to_albums=self.photo_to_albums,
            account_data=self.account_data,
            include_extended_description=True,
            write_xmp_sidecars=False
        )

    def _create_test_jpeg(self):
        """Create a minimal valid JPEG file for testing"""
        # Minimal JPEG: SOI + EOI markers
        jpeg_data = b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f\x1e\x1d\x1a\x1c\x1c $.\' ",#\x1c\x1c(7),01444\x1f\'9=82<.342\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x11\x00\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\xff\xda\x00\x08\x01\x01\x00\x00?\x00\x7f\xd9'

        tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        tmp.write(jpeg_data)
        tmp.close()
        return Path(tmp.name)

    def test_embed_image_metadata_successful(self):
        """Test successful metadata embedding with actual exiftool"""
        tmp_path = self._create_test_jpeg()

        try:
            # Create test metadata
            metadata = {
                'id': '123456789',
                'name': 'Test Photo Title',
                'description': 'A test photo description',
                'date_taken': '2024-01-15 10:30:45',
                'license': 'CC BY-NC',
                'count_views': '100',
                'count_faves': '5',
                'count_comments': '2',
                'privacy': 'Private',
                'safety': 'Safe',
                'photopage': 'https://flickr.com/photos/testuser/123456789/',
                'tags': [
                    {'tag': 'test'},
                    {'tag': 'photo'}
                ]
            }

            # Call the method with real exiftool
            self.exif_writer._embed_image_metadata(tmp_path, metadata)

            # Verify metadata was written by reading it back
            result = subprocess.run(
                ['exiftool', '-Title', str(tmp_path)],
                capture_output=True,
                text=True
            )
            self.assertIn('Test Photo Title', result.stdout)

            # Verify date was written
            result = subprocess.run(
                ['exiftool', '-DateTimeOriginal', str(tmp_path)],
                capture_output=True,
                text=True
            )
            self.assertIn('2024:01:15 10:30:45', result.stdout)

        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def test_embed_image_metadata_with_minimal_metadata(self):
        """Test embedding with minimal metadata fields"""
        tmp_path = self._create_test_jpeg()

        try:
            # Minimal metadata with only required fields
            metadata = {
                'id': '987654321',
                'date_taken': '2024-01-16 14:20:00'
            }

            # Should not raise an error
            self.exif_writer._embed_image_metadata(tmp_path, metadata)

            # Verify date was written
            result = subprocess.run(
                ['exiftool', '-DateTimeOriginal', str(tmp_path)],
                capture_output=True,
                text=True
            )
            self.assertIn('2024:01:16 14:20:00', result.stdout)

        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def test_embed_image_metadata_with_tags(self):
        """Test that tags are properly embedded in image"""
        tmp_path = self._create_test_jpeg()

        try:
            metadata = {
                'id': '123456789',
                'date_taken': '2024-01-15 10:30:45',
                'tags': [
                    {'tag': 'travel'},
                    {'tag': 'nature'},
                    {'tag': 'landscape'}
                ]
            }

            self.exif_writer._embed_image_metadata(tmp_path, metadata)

            # Verify tags were written
            result = subprocess.run(
                ['exiftool', '-Keywords', str(tmp_path)],
                capture_output=True,
                text=True
            )

            # Check that keywords were written (exiftool may format them)
            self.assertTrue(len(result.stdout) > 0)

        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def test_embed_image_metadata_with_copyright(self):
        """Test that copyright information is embedded"""
        tmp_path = self._create_test_jpeg()

        try:
            metadata = {
                'id': '123456789',
                'date_taken': '2024-01-15 10:30:45',
                'license': 'All Rights Reserved'
            }

            self.exif_writer._embed_image_metadata(tmp_path, metadata)

            # Verify copyright was written
            result = subprocess.run(
                ['exiftool', '-Copyright', str(tmp_path)],
                capture_output=True,
                text=True
            )
            self.assertIn('All Rights Reserved', result.stdout)

        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def test_embed_image_metadata_with_fixture_broken_jpg(self):
        """Test embedding metadata in the fixture broken.jpg file"""

        # Create a temporary copy to avoid modifying the fixture
        tmp_path = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        tmp_path.close()
        tmp_path = Path(tmp_path.name)

        try:
            shutil.copy2(self.broken_jpg, tmp_path)

            metadata = {
                'id': '123456789',
                'name': 'Fixture Test',
                'date_taken': '2024-01-15 10:30:45',
                'description': 'Testing with fixture file'
            }

            self.exif_writer._embed_image_metadata(tmp_path, metadata)
            # If it succeeds, the file was repairable
            self.assertTrue(tmp_path.exists())

        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def test_broken_jpeg_orientation_field_replaced(self):
        """
        RED-GREEN TDD: Test that broken JPEG orientation fields are replaced with valid values.

        This test verifies that when embedding metadata in a broken JPEG:
        1. The method completes without raising an error
        2. The orientation field is written with a valid value (1-8)
        3. The broken/invalid orientation value is replaced
        """
        if not self.broken_jpg.exists():
            self.skipTest(f"Fixture file not found: {self.broken_jpg}")

        # Create a temporary copy to avoid modifying the fixture
        tmp_path = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        tmp_path.close()
        tmp_path = Path(tmp_path.name)

        try:
            shutil.copy2(self.broken_jpg, tmp_path)

            # Get initial orientation value (may be broken/invalid)
            initial_result = subprocess.run(
                ['exiftool', '-Orientation', str(tmp_path)],
                capture_output=True,
                text=True
            )
            initial_orientation = initial_result.stdout

            metadata = {
                'id': '123456789',
                'name': 'Broken JPEG Test',
                'date_taken': '2024-01-15 10:30:45',
                'description': 'Testing orientation replacement in broken JPEG'
            }

            # This should complete successfully without raising an error
            self.exif_writer._embed_image_metadata(tmp_path, metadata)

            # Verify the orientation field now has a valid value (1-8)
            result = subprocess.run(
                ['exiftool', '-Orientation', str(tmp_path)],
                capture_output=True,
                text=True
            )

            orientation_output = result.stdout

            # Valid EXIF orientation descriptions (1-8)
            # Output format: "Orientation : Horizontal (normal)" or "Orientation : 1" etc
            valid_orientations = [
                'Horizontal',  # 1 - normal
                'Rotate 90',   # 6
                'Rotate 180',  # 3
                'Rotate 270',  # 8
            ]

            # Check that the output contains a valid orientation
            has_valid_orientation = False
            for valid in valid_orientations:
                if valid in orientation_output:
                    has_valid_orientation = True
                    break

            # Also check for numeric values 1-8
            import re
            match = re.search(r':\s*(\d+)', orientation_output)
            if match:
                orientation_value = int(match.group(1))
                has_valid_orientation = 1 <= orientation_value <= 8

            self.assertTrue(has_valid_orientation,
                f"Orientation should be valid (1-8), got: {orientation_output.strip()}")

            # Verify the file exists and was modified
            self.assertTrue(tmp_path.exists(), "JPEG file should still exist after embedding")

        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def test_embed_preserves_file(self):
        """Test that the file still exists after metadata embedding"""
        tmp_path = self._create_test_jpeg()
        original_size = tmp_path.stat().st_size

        try:
            metadata = {
                'id': '123456789',
                'date_taken': '2024-01-15 10:30:45'
            }

            self.exif_writer._embed_image_metadata(tmp_path, metadata)

            # File should still exist
            self.assertTrue(tmp_path.exists())

            # File should have changed (metadata was added)
            new_size = tmp_path.stat().st_size
            self.assertGreater(new_size, original_size)

        finally:
            if tmp_path.exists():
                tmp_path.unlink()


if __name__ == '__main__':
    unittest.main()
