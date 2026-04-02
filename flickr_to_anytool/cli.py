"""
CLI interface for Flickr2Any Tool
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from .constants import INCLUDE_EXTENDED_DESCRIPTION, WRITE_XMP_SIDECARS, log_memory_usage
from .preprocessor import FlickrPreprocessor
from .exporter import FlickrToImmich


def setup_directory_widgets(preprocessing, main_settings):
    """Add directory widgets to existing argument groups"""

    # Add source directory widget
    source_dir = preprocessing.add_argument(
        '--source-dir',
        metavar='Source Directory',
        help='Directory for Flickr zip files'
    )

    # Add metadata directory widget
    metadata_dir = main_settings.add_argument(
        '--metadata-dir',
        metavar='Metadata Directory',
        required=True,
        help='Directory for metadata files'
    )

    # Add photos directory widget
    photos_dir = main_settings.add_argument(
        '--photos-dir',
        metavar='Photos Directory',
        required=True,
        help='Directory for photos'
    )

    # Add output directory widget
    output_dir = main_settings.add_argument(
        '--output-dir',
        metavar='Output Directory',
        required=True,
        help='Directory for output files'
    )

    # Add results directory widget
    results_dir = main_settings.add_argument(
        '--results-dir',
        metavar='Results Directory',
        help='Directory for results files'
    )

    return preprocessing, main_settings


def main():
    parser = argparse.ArgumentParser(description='Flickr to Any Tool')

    # Create groups
    preprocessing = parser.add_argument_group(
        'Step 1: Preprocessing',
        'Extract Flickr export zip files'
    )

    # Add other non-directory arguments to preprocessing
    preprocessing.add_argument(
        '--zip-preprocessing',
        action='store_true',
        help='Enable if you need to extract Flickr export zip files first',
        default=True
    )

    main_settings = parser.add_argument_group(
        'Step 2: Main Settings',
        'Configure main conversion options'
    )
    # Setup directory widgets
    preprocessing, main_settings = setup_directory_widgets(preprocessing, main_settings)

    # Create export type group
    export_type = parser.add_argument_group(
        'Step 3: Export Type',
        'Choose what to export'
    )

    # Main settings non-directory arguments
    export_type.add_argument(
        '--organization',
        metavar='Organization Method',
        choices=['by_album', 'by_date'],
        default='by_date',
        help='How to organize photos in the library'
    )
    export_type.add_argument(
        '--date-format',
        metavar='Date Format',
        choices=['yyyy', 'yyyy-mm', 'yyyy/yyyy-mm-dd', 'yyyy/yyyy-mm', 'yyyy-mm-dd'],
        default='yyyy/yyyy-mm',
        help='Date format for folder structure'
    )

    export_type.add_argument(
        '--export-mode',
        metavar='What to Export',
        choices=[
            'Full library and Highlights',
            'Full library only',
            'Highlights only'
        ],
        default='Full library and Highlights',
        help='Choose what to export from your Flickr library'
    )

    # Interesting photos configuration
    export_type.add_argument(
        '--interesting-period',
        metavar='Interesting Time Period',
        choices=['all-time', 'byyear'],
        default='all-time',
        help='Time period for interesting photos'
    )
    export_type.add_argument(
        '--interesting-count',
        metavar='Number of Photos',
        type=int,
        default=100,
        help='Number of interesting photos to fetch (max 500)'
    )

    # Add highlight weighting settings
    export_type.add_argument(
        '--fave-weight',
        metavar='Favorite Weight',
        type=float,
        default=2,
        help='Weight multiplier for favorites (default: 10.0)'
    )

    export_type.add_argument(
        '--comment-weight',
        metavar='Comment Weight',
        type=float,
        default=1,
        help='Weight multiplier for comments (default: 5.0)'
    )

    export_type.add_argument(
        '--view-weight',
        metavar='View Weight',
        type=float,
        default=2,
        help='Weight multiplier for views (default: 0.1)'
    )

    # Add minimum threshold settings
    export_type.add_argument(
        '--min-views',
        metavar='Minimum Views',
        type=int,
        default=20,
        help='Minimum views required (default: 20)'
    )

    export_type.add_argument(
        '--min-faves',
        metavar='Minimum Favorites',
        type=int,
        default=1,
        help='Minimum favorites required (default: 0)'
    )

    export_type.add_argument(
        '--min-comments',
        metavar='Minimum Comments',
        type=int,
        default=1,
        help='Minimum comments required (default: 0)'
    )

    # Advanced options
    advanced = parser.add_argument_group(
        'Step 4: Advanced Options',
        'Configure additional settings'
    )

    advanced.add_argument(
        '--no-extended-description',
        action='store_true',
        help='Only include original description'
    )
    advanced.add_argument(
        '--no-xmp-sidecars',
        action='store_true',
        help='Skip writing XMP sidecar files'
    )
    advanced.add_argument(
        '--export-block-if-failure',
        action='store_true',
        help='Stop if metadata processing fails'
    )
    advanced.add_argument(
        '--resume',
        action='store_true',
        help='Skip existing files'
    )
    advanced.add_argument(
        '--quiet',
        action='store_true',
        default=True,
        help='Reduce console output'
    )
    advanced.add_argument(
        '--use-api',
        action='store_true',
        default=False,
        help='Use Flickr API for looking up comments and favorites'
    )
    advanced.add_argument(
        '--api-key',
        metavar='Flickr API Key',
        help='Enter your Flickr API key',
        default=''
    )
    advanced.add_argument(
        '--api-secret',
        metavar='Flickr API Secret',
        help='Enter your Flickr API secret',
        default=''
    )

    advanced.add_argument(
        '--cpu-cores',
        metavar='CPU Cores',
        type=int,
        default=min(os.cpu_count(), 4),  # Default to lesser of 4 or available cores
        help=f'Number of CPU cores to use (default: {min(os.cpu_count(), 4)}, max: {os.cpu_count()})'
    )

    advanced.add_argument(
        '--max-memory-percent',
        metavar='Max Memory %',
        type=int,
        default=75,
        help='Maximum percentage of system memory to use'
    )

    advanced.add_argument(
        '--batch-size',
        metavar='Batch Size',
        type=int,
        default=2500,
        help='Number of photos to process in each batch (lower = less memory use but slower). Min 100, Max 5000'
    )

    # Parse arguments
    args = parser.parse_args()

    # Then validate directories
    required_dirs = {
        'Metadata Directory': args.metadata_dir,
        'Photos Directory': args.photos_dir,
        'Output Directory': args.output_dir
    }

    missing_dirs = [name for name, path in required_dirs.items()
                    if path and not os.path.exists(os.path.dirname(path))]

    if missing_dirs:
        error_msg = f"Please set the following directories: {', '.join(missing_dirs)}"
        logging.error(error_msg)
        return args

    # Get the script's directory
    script_dir = Path(__file__).parent
    log_file = script_dir / 'flickr_to_immich.log'

    # Remove any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Create handlers
    file_handler = logging.FileHandler(log_file)  # Changed from args.log_file to log_file
    console_handler = logging.StreamHandler(sys.stdout)

    # Create formatter
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to root logger
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)

    # Set log level
    logging.root.setLevel(logging.INFO)

    logging.info("Logging initialized")

    try:
        # Handle preprocessing first
        if args.zip_preprocessing and args.source_dir:
            preprocessor = FlickrPreprocessor(
                source_dir=args.source_dir,
                metadata_dir=args.metadata_dir,
                photos_dir=args.photos_dir,
                quiet=args.quiet
            )
            preprocessor.process_exports()

        # Handle API key and secret
        if args.use_api:
            api_key = args.api_key or os.environ.get('FLICKR_API_KEY')
            api_secret = args.api_secret or os.environ.get('FLICKR_API_SECRET')
            if not api_key or not api_secret:
                logging.warning("Flickr API enabled but API key and/or secret not provided")
                api_key = None
                api_secret = None
        else:
            api_key = None
            api_secret = None

        # Create converter instance
        converter = FlickrToImmich(
            metadata_dir=args.metadata_dir,
            photos_dir=args.photos_dir,
            output_dir=args.output_dir,
            date_format=args.date_format,
            api_key=api_key,
            api_secret=api_secret,
            log_file=str(log_file),
            results_dir=args.results_dir,
            include_extended_description=not args.no_extended_description,
            write_xmp_sidecars=not args.no_xmp_sidecars,
            block_if_failure=args.export_block_if_failure,
            resume=args.resume,
            use_api=args.use_api,
            quiet=args.quiet,
            fave_weight=args.fave_weight,
            comment_weight=args.comment_weight,
            view_weight=args.view_weight,
            min_views=args.min_views,
            min_faves=args.min_faves,
            min_comments=args.min_comments,
            max_memory_percent=args.max_memory_percent,
            batch_size=args.batch_size,
            cpu_cores=args.cpu_cores
        )

        # Process based on export mode
        if args.export_mode == 'Full library and Highlights':
            logging.info("Processing both full library and highlights...")

            # Process library
            logging.info("Step 2: Processing full library...")
            print("\nProcessing full library...")
            converter.process_photos(args.organization, args.date_format)

            # Process highlights
            logging.info("Step 1: Creating interesting albums...")
            converter.create_interesting_albums(
                args.interesting_period,
                args.interesting_count
            )

            # Print statistics only once at the end
            converter.print_statistics()

        elif args.export_mode == 'Full library only':
            logging.info("Processing full library only...")
            print("\nProcessing full library...")
            converter.process_photos(args.organization, args.date_format)
            converter.print_statistics()

        elif args.export_mode == 'Highlights only':
            logging.info("Processing highlights only...")
            converter.create_interesting_albums(
                args.interesting_period,
                args.interesting_count)
            converter.print_statistics()

        # Print final statistics
        converter.write_results_log()

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.stdout.flush()
        raise

    return args

