from typing import Dict
from datetime import datetime
import logging
import os
from typing import List, Optional, Tuple, TypedDict

from flickrapi import FlickrAPI
import flickrapi

class FavouriteRecord(TypedDict):
    username: str
    nsid: str
    favedate: str

class FlickrApiMetadata:
    def __init__(self, flickr_api: FlickrAPI):
        self.flickr = flickr_api
        self.faves: Dict[str, List[FavouriteRecord]] = dict()
        self.user_info_cache: Dict[str, Tuple[str, str]] = dict()

    @staticmethod
    def auth(api_key: str, api_secret: str) -> Optional['FlickrApiMetadata']:
        if not (api_key and api_secret):
            return None

        try:
            flickr_api = flickrapi.FlickrAPI(api_key, api_secret, format='etree')
            logging.getLogger('flickrapi').setLevel(logging.ERROR)
            if not flickr_api.token_valid(perms='read'):
                flickr_api.get_request_token(oauth_callback='oob')
                authorize_url = flickr_api.auth_url(perms='read')
                print(f"\nAuthorise this app by opening the following URL in your browser:\n{authorize_url}\n")
                verifier = input("Enter the verifier code from Flickr: ").strip()
                flickr_api.get_access_token(verifier)
                logging.info("Successfully authenticated with Flickr API")
                return FlickrApiMetadata(flickr_api)
        except Exception as e:
            logging.warning(f"Failed to authenticate with Flickr API: {e}")
            logging.warning("Comments and favorites lookup will be limited")
            return None

    def _get_user_info(self, user_id: str) -> Tuple[str, str]:
        """
        Get username and real name for a user ID using Flickr API
        Returns tuple of (username, realname)
        If API is unavailable or user not found, returns (user_id, "")
        """
        # Check cache first
        if user_id in self.user_info_cache:
            return self.user_info_cache[user_id]

        # Fall back to just user_id if no API available
        if not self.flickr:
            return (user_id, "")

        try:
            # Call Flickr API
            user_info = self.flickr.people.getInfo(api_key=os.environ['FLICKR_API_KEY'], user_id=user_id)

            # Parse response
            person = user_info.find('person')
            if person is None:
                raise ValueError("No person element found in response")

            username = person.find('username').text
            realname = person.find('realname')
            realname = realname.text if realname is not None else ""

            # Cache the result
            self.user_info_cache[user_id] = (username, realname)
            return (username, realname)

        except Exception as e:
            logging.warning(f"Failed to get user info for {user_id}: {e}")
            return (user_id, "")

    def _get_photo_favorites(self, photo_id: str) -> List[Dict]:
        """Fetch list of users who favorited a photo using Flickr API"""
        try:
            favorites = []
            page = 1
            per_page = 50

            while True:
                try:
                    # Get favorites for current page
                    response = self.flickr.photos.getFavorites(
                        api_key=os.environ['FLICKR_API_KEY'],
                        photo_id=photo_id,
                        page=page,
                        per_page=per_page
                    )

                    # Extract person elements
                    photo_elem = response.find('photo')
                    if photo_elem is None:
                        break

                    person_elems = photo_elem.findall('person')
                    if not person_elems:
                        break

                    # Process each person
                    for person in person_elems:
                        username = person.get('username', '')
                        nsid = person.get('nsid', '')
                        favedate = person.get('favedate', '')

                        # Convert favedate to readable format
                        if favedate:
                            try:
                                favedate = datetime.fromtimestamp(int(favedate)).strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                pass  # Keep original if conversion fails

                        favorites.append({
                            'username': username,
                            'nsid': nsid,
                            'favedate': favedate
                        })

                    # Check if we've processed all pages
                    total_pages = int(photo_elem.get('pages', '1'))
                    if page >= total_pages:
                        break

                    page += 1

                except Exception as e:
                    # If we encounter an error on a specific page, log it at debug level and break
                    logging.debug(f"Error fetching favorites page {page} for photo {photo_id}: {str(e)}")
                    break

            return favorites

        except Exception as e:
            # Log at debug level instead of warning since this is expected for some photos
            logging.debug(f"Failed to get favorites for photo {photo_id}: {str(e)}")
            return []


    def _load_photo_metadata(self, photo_id: str) -> Optional[Dict]:
       """Load metadata for a specific photo"""
       metadata = self._load_json_metadata(photo_id)
       if not metadata:
           return None

       if self.use_api and self.flickr:
           try:
               photo_info = self.flickr.photos.getInfo(
                   api_key=os.environ['FLICKR_API_KEY'],
                   photo_id=photo_id
               )
               """
               self._extract_privacy_from_api(photo_info, metadata)
               self._extract_albums_from_api(photo_id, metadata)
               self._extract_orientation_from_api(photo_info, metadata)
               """

           except Exception as e:
               if not self.quiet:
                   logging.debug(f"API fetch failed for photo {photo_id}")

       return metadata


    # THIS SECTION ENABLES ADDITIONAL METADATA TO BE ACCESSED FROM API IN PLACE OF THE JSON FILES, WHICH ARE UNRELIABLE
    def _get_metadata_from_api(self, photo_id: str) -> Optional[Dict]:
        """Get metadata from Flickr API with JSON fallback"""
        logging.debug(f"\nAttempting API fetch for photo {photo_id}")
        logging.debug(f"Flickr API initialized: {self.flickr is not None}")
        logging.debug(f"API Key present: {bool(os.environ.get('FLICKR_API_KEY'))}")

        if not self.flickr:
            logging.debug("No Flickr API connection, falling back to JSON")
            return self._load_json_metadata(photo_id)

        try:
            # Load base metadata from JSON
            metadata = self._load_json_metadata(photo_id) or {}

            # Get photo info from API
            photo_info = self.flickr.photos.getInfo(
                api_key=os.environ['FLICKR_API_KEY'],
                photo_id=photo_id
            )

            # Extract all API fields using dedicated methods
            self._extract_privacy_from_api(photo_info, metadata)
            self._extract_albums_from_api(photo_id, metadata)
            #self._extract_orientation_from_api(photo_info, metadata)

            return metadata

        except Exception as e:
            logging.debug(f"API fetch failed for photo {photo_id}, using JSON: {str(e)}")
            return self._load_json_metadata(photo_id)


    """ UNTESTED CODE

    def _extract_privacy_from_api(self, photo_info, metadata: Dict):
        #Extract privacy settings from API response using privacy level mapping
        visibility = photo_info.find('photo/visibility')
        if visibility is not None:
            privacy_level = 5  # Default to private (5)

            if int(visibility.get('ispublic', 0)):
                privacy_level = 1
            elif int(visibility.get('isfriend', 0)) and int(visibility.get('isfamily', 0)):
                privacy_level = 4
            elif int(visibility.get('isfriend', 0)):
                privacy_level = 2
            elif int(visibility.get('isfamily', 0)):
                privacy_level = 3

            # Map privacy level to string
            privacy_map = {
                1: 'public',
                2: 'friends only',
                3: 'family only',
                4: 'friends & family',
                5: 'private'
            }
            metadata['privacy'] = privacy_map[privacy_level]
            metadata['privacy_level'] = privacy_level  # Store numeric level for reference

    def _extract_albums_from_api(self, photo_id: str, metadata: Dict):
        #Extract album information using photosets.getPhotos
        try:
            albums = []
            # First get all photosets (albums) for the user
            photosets = self.flickr.photosets.getList(
                api_key=os.environ['FLICKR_API_KEY'],
                user_id=self.account_data.get('nsid')
            )

            # For each photoset, check if our photo is in it
            for photoset in photosets.findall('.//photoset'):
                photoset_id = photoset.get('id')
                photos = self.flickr.photosets.getPhotos(
                    api_key=os.environ['FLICKR_API_KEY'],
                    photoset_id=photoset_id,
                    user_id=self.account_data.get('nsid')
                )

                # Check if our photo_id is in this photoset
                for photo in photos.findall('.//photo'):
                    if photo.get('id') == photo_id:
                        albums.append(photoset.find('title').text)
                        break  # Found in this album, no need to check rest of photos

            if albums:
                self.photo_to_albums[photo_id] = albums
                metadata['albums'] = albums

        except Exception as e:
            logging.debug(f"Failed to get albums for {photo_id}: {str(e)}")
    """
