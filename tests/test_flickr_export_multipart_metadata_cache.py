
import unittest
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flickr_to_anytool.flickr_export_multipart_metadata_cache import FlickrExportMultipartMetadataCache


class TestFlickrExportMultipartMetadataCacheAlbums(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up fixture directory"""
        cls.fixture_dir = Path(__file__).parent / 'fixtures' / 'album_metadata'

    def setUp(self):
        """Set up test instance with fixture directory"""
        self.cache = FlickrExportMultipartMetadataCache(
            metadata_dir=self.fixture_dir,
            base_name='albums',
            key='albums'
        )

    def test_albums_returns_list(self):
        """Test that albums() returns a list"""
        albums = self.cache.albums()
        self.assertIsInstance(albums, list)

    def test_albums_loads_correct_count(self):
        """Test that albums() loads the correct number of albums"""
        albums = self.cache.albums()
        self.assertEqual(len(albums), 2)

    def test_albums_contains_required_fields(self):
        """Test that each album contains required fields"""
        albums = self.cache.albums()
        required_fields = ['id', 'title', 'description', 'photos', 'photo_count']

        for album in albums:
            for field in required_fields:
                self.assertIn(field, album, f"Album missing required field: {field}")

    def test_first_album_data_integrity(self):
        """Test that the first album is loaded correctly"""
        albums = self.cache.albums()
        first_album = albums[0]

        self.assertEqual(first_album['id'], '29495325938624887')
        self.assertEqual(first_album['title'], 'Bob 40th')
        self.assertEqual(first_album['description'], 'Uploaded from mac')
        self.assertEqual(first_album['photo_count'], '2')
        self.assertEqual(len(first_album['photos']), 2)

    def test_second_album_data_integrity(self):
        """Test that the second album is loaded correctly"""
        albums = self.cache.albums()
        second_album = albums[1]

        self.assertEqual(second_album['id'], '33023451987942148')
        self.assertEqual(second_album['title'], 'Old Family Photos')
        self.assertEqual(second_album['description'], '')
        self.assertEqual(second_album['photo_count'], '1')
        self.assertEqual(len(second_album['photos']), 1)

    def test_album_photos_are_strings(self):
        """Test that photo IDs in albums are stored as strings"""
        albums = self.cache.albums()

        for album in albums:
            for photo_id in album['photos']:
                self.assertIsInstance(photo_id, str)

    def test_albums_with_nonexistent_dir_raises_error(self):
        """Test that albums() raises FileNotFoundError for non-existent directory"""
        nonexistent_dir = Path('/nonexistent/directory')

        with self.assertRaises(FileNotFoundError):
            cache = FlickrExportMultipartMetadataCache(
                metadata_dir=nonexistent_dir,
                base_name='albums',
                key='albums'
            )

