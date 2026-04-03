"""
Unit tests for FlickrToImmich exporter module
"""

import unittest
from pathlib import Path
import json
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flickr_to_anytool.exporter import FlickrToImmich


class TestExtractPhotoId(unittest.TestCase):
    """Test cases for FlickrToImmich._extract_photo_id method"""

    @classmethod
    def setUpClass(cls):
        """Set up fixture directory and files"""
        cls.fixture_dir = Path(__file__).parent / 'fixtures' / 'metadata'
        cls.fixture_dir.mkdir(parents=True, exist_ok=True)

        # Create fixture metadata files for testing
        fixture_files = {
            '144332211': {'id': '144332211', 'name': 'test_photo_1'},
            '123456789': {'id': '123456789', 'name': 'test_photo_2'},
            '987654321': {'id': '987654321', 'name': 'test_photo_3'},
        }

        for photo_id, metadata in fixture_files.items():
            fixture_file = cls.fixture_dir / f'photo_{photo_id}.json'
            if not fixture_file.exists():
                with open(fixture_file, 'w') as f:
                    json.dump(metadata, f)

    def setUp(self):
        """Set up test instance with actual fixture directory"""
        self.instance = FlickrToImmich.__new__(FlickrToImmich)
        self.instance.metadata_dir = self.fixture_dir

    def test_extract_standard_format(self):
        """Test extraction of standard Flickr filename format"""
        # Standard format: photo_12345678901.jpg
        result = self.instance._extract_photo_id('photo_12345678901.jpg')
        self.assertEqual(result, '12345678901')

    def test_extract_with_o_suffix(self):
        """Test extraction from filename with _o suffix (original)"""
        # Format: photo_12345678901_o.jpg
        result = self.instance._extract_photo_id('photo_12345678901_o.jpg')
        self.assertEqual(result, '12345678901')

    def test_extract_case_insensitive(self):
        """Test that extraction is case insensitive"""
        # Uppercase filename
        result = self.instance._extract_photo_id('PHOTO_12345678901_O.JPG')
        self.assertEqual(result, '12345678901')

    def test_extract_10_digit_id(self):
        """Test extraction of 10-digit photo ID"""
        result = self.instance._extract_photo_id('photo_1234567890.jpg')
        self.assertEqual(result, '1234567890')

    def test_extract_11_digit_id(self):
        """Test extraction of 11-digit photo ID"""
        result = self.instance._extract_photo_id('photo_12345678901.jpg')
        self.assertEqual(result, '12345678901')

    def test_no_id_in_filename(self):
        """Test that None is returned when no ID is found"""
        result = self.instance._extract_photo_id('random_photo_file.jpg')
        self.assertIsNone(result)

    def test_can_extract_id_when_file_has_o_suffix(self):
        """Test that ID can be extracted from filename with _o suffix"""
        result = self.instance._extract_photo_id('img_5871_101631362_o.jpg')
        self.assertEqual(result, '101631362')

    def test_id_too_short(self):
        """Test that IDs shorter than 8 digits are ignored"""
        result = self.instance._extract_photo_id('photo_1234567.jpg')
        self.assertIsNone(result)

    def test_id_too_long(self):
        """Test that IDs longer than 11 digits are ignored"""
        result = self.instance._extract_photo_id('photo_123456789012.jpg')
        self.assertIsNone(result)


    def test_multiple_ids_no_metadata_returns_largest(self):
        """Test that largest ID is returned when multiple IDs exist and none have metadata"""
        # Use IDs that don't have metadata files
        result = self.instance._extract_photo_id('11111111111_22222222222.jpg')
        # Should return the larger ID
        self.assertEqual(result, '22222222222')

    def test_single_id_returned_without_metadata_check(self):
        """Test that single ID is returned even if no metadata exists"""
        # Use an ID that doesn't have a metadata file
        result = self.instance._extract_photo_id('photo_12345678901.jpg')
        self.assertEqual(result, '12345678901')

    def test_id_with_underscore_separators(self):
        """Test extraction from filename with multiple underscores"""
        result = self.instance._extract_photo_id('my_photo_12345678901_desc.jpg')
        self.assertEqual(result, '12345678901')

    def test_hyphen_separated_id(self):
        """Test extraction from filename with hyphens"""
        result = self.instance._extract_photo_id('my-photo-12345678901-desc.jpg')
        self.assertEqual(result, '12345678901')

    def test_multiple_digit_groups_extracts_correct_id(self):
        """Test extraction when filename contains multiple digit groups"""
        result = self.instance._extract_photo_id('2024_01_15_12345678901_photo.jpg')
        # Should extract the 11-digit ID, not the 4, 2, or 2-digit numbers
        self.assertEqual(result, '12345678901')

    def test_id_at_start_of_filename_with_hex_secret(self):
        """Test extraction from Flickr format where ID is at the start: {id}_{hex_secret}_o.jpg"""
        result = self.instance._extract_photo_id('52759384619_be0461e77c_o.jpg')
        self.assertEqual(result, '52759384619')


if __name__ == '__main__':
    unittest.main()
