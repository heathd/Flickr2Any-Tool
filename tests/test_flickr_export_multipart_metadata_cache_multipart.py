import unittest
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flickr_to_anytool.flickr_export_multipart_metadata_cache import FlickrExportMultipartMetadataCache

class TestFlickrExportMultipartMetadataCacheMultipart(unittest.TestCase):
    """Test cases for multipart albums loading behavior"""

    @classmethod
    def setUpClass(cls):
        """Set up fixture directory for multipart tests"""
        cls.fixture_dir = Path(__file__).parent / 'fixtures' / 'album_metadata_multipart'

    def setUp(self):
        """Set up test instance with multipart fixture directory"""
        self.cache = FlickrExportMultipartMetadataCache(
            metadata_dir=self.fixture_dir,
            base_name='albums',
            key='albums'
        )

    def test_multipart_loads_both_files(self):
        """Test that multipart files are loaded and merged"""
        albums = self.cache.albums()
        self.assertEqual(len(albums), 2)

    def test_multipart_preserves_all_albums(self):
        """Test that all albums from both parts are preserved"""
        albums = self.cache.albums()
        album_ids = [album['id'] for album in albums]

        self.assertIn('29495325938624887', album_ids)
        self.assertIn('33023451987942148', album_ids)

    def test_multipart_first_part_data(self):
        """Test that first part data is correctly loaded"""
        albums = self.cache.albums()
        first_album = next(a for a in albums if a['id'] == '29495325938624887')

        self.assertEqual(first_album['title'], 'Bob 40th')
        self.assertEqual(len(first_album['photos']), 2)

    def test_multipart_second_part_data(self):
        """Test that second part data is correctly loaded"""
        albums = self.cache.albums()
        second_album = next(a for a in albums if a['id'] == '33023451987942148')

        self.assertEqual(second_album['title'], 'Old Family Photos')
        self.assertEqual(len(second_album['photos']), 1)

    def test_multipart_combines_album_lists(self):
        """Test that album lists from multiple parts are properly combined"""
        albums = self.cache.albums()
        all_photo_ids = []

        for album in albums:
            all_photo_ids.extend(album['photos'])

        expected_photo_ids = ['56824916781', '56824916782', '56824916783']
        self.assertEqual(all_photo_ids, expected_photo_ids)

