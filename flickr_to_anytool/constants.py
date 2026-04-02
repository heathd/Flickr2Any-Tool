"""
Constants and enums for Flickr2Any Tool
"""

from enum import Enum
import logging
import psutil


class MediaType(Enum):
    """Supported media types"""
    IMAGE = "image"
    VIDEO = "video"
    UNKNOWN = "unknown"


# Configuration flags
INCLUDE_EXTENDED_DESCRIPTION = True  # Set to False to only include original description
WRITE_XMP_SIDECARS = True  # Set to False to skip writing XMP sidecar files

EXIF_ORIENTATION_MAP = {
    1: {
        'text': 'top, left',
        'description': 'Horizontal (normal)',
        'rotation': 0,
        'mirrored': False
    },
    2: {
        'text': 'top, right',
        'description': 'Mirror horizontal',
        'rotation': 0,
        'mirrored': True
    },
    3: {
        'text': 'bottom, right',
        'description': 'Rotate 180',
        'rotation': 180,
        'mirrored': False
    },
    4: {
        'text': 'bottom, left',
        'description': 'Mirror vertical',
        'rotation': 180,
        'mirrored': True
    },
    5: {
        'text': 'left, right',
        'description': 'Mirror horizontal and rotate 270 CW',
        'rotation': 270,
        'mirrored': True
    },
    6: {
        'text': 'right, top',
        'description': 'Rotate 90 CW',
        'rotation': 90,
        'mirrored': False
    },
    7: {
        'text': 'right, bottom',
        'description': 'Mirror horizontal and rotate 90 CW',
        'rotation': 90,
        'mirrored': True
    },
    8: {
        'text': 'left, top',
        'description': 'Rotate 270 CW',
        'rotation': 270,
        'mirrored': False
    }
}


def log_memory_usage():
    """Log current memory usage"""
    process = psutil.Process()
    memory = process.memory_info()
    logging.info(f"Memory usage: {memory.rss / (1024 * 1024):.1f} MB")
