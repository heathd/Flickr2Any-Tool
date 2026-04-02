"""
**** FLICKR to ANY TOOL ****
by Rob Brown
https://github.com/brownphotographic/Flickr2Any-Tool

Copyright (C) 2025 Robert Brown

This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

For usage instructions please see the README file.
"""

import logging
import os
import sys

# Configure environment for progress bars and disable most logging
os.environ['TQDM_DISABLE'] = 'false'
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Disable all logging except critical
logging.getLogger().setLevel(logging.CRITICAL)
logging.getLogger('flickrapi').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('PIL').setLevel(logging.CRITICAL)

from .cli import main

if __name__ == '__main__':
    main()
