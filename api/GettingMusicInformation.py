import json
import requests
import time
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(category)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        'enrichment.log',
        maxBytes=10 * 1024 * 1024, 
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


setup_logging()
logger = logging.getLogger()
extra = {'category': 'MAIN'}


class GeniusClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "API"
        })

    def get_song_info(self, query):
        try:
            start_time = time.time()
            search_res = self.session.get(
                "https://api.genius.com/search",
                params={"q": query}
            )
            search_res.raise_for_status()

            logger.info(
                f"Search '{query[:40]}' ({time.time() - start_time:.1f}s) - status {search_res.status_code}",
                extra={'category': 'API'}
            )

            search_data = search_res.json()
            if not search_data.get("response", {}).get("hits"):
                logger.warning(f"Not find '{query[:40]}'", extra={'category': 'API'})
                return None

            song_id = search_data["response"]["hits"][0]["result"]["id"]

            song_res = self.session.get(f"https://api.genius.com/songs/{song_id}")
            song_res.raise_for_status()

            logger.info(
                f"Track find ({time.time() - start_time:.1f}s) - Status {song_res.status_code}",
                extra={'category': 'API'}
            )

            return song_res.json()["response"]["song"]

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP Error: {e.response.status_code} - {e.response.text[:200]}",
                extra={'category': 'API'}
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error: {str(e)[:200]}",
                exc_info=True,
                extra={'category': 'API'}
            )
            return None

def save_data(data):
    start = time.time()
    try:
        with open('artist_with_tracks_enriched.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(
            f"Data saved ({time.time() - start:.1f}s)",
            extra={'category': 'SAVE'}
        )
    except Exception as e:
        logger.error(
            f"Save failed: {str(e)}",
            extra={'category': 'SAVE'}
        )


start_time = time.time()
logger.info("Starting enrichment process", extra=extra)
try:
    with open('artist_with_tracks_enriched.json', 'r', encoding='utf-8') as f:
        artists = json.load(f)

    total_artists = len(artists)
    total_tracks = sum(len(a['tracks']) for a in artists)
    logger.info(
        f"Loaded {total_artists} artists with {total_tracks} tracks",
        extra=extra
    )

    client = GeniusClient("s1H8Z1hm8O_r4Ruc_TU9cODT6-oocsujuf5qungGzWkHqCVHxFeHDcfIKsfUOABL")

    stats = {'processed': 0, 'skipped': 0, 'errors': 0}
    TARGET_FIELDS = ['language', 'release_date', 'album_name',
                     'producer_artists', 'writer_artists', 'featured_artists']

    for artist_idx, artist in enumerate(artists, 1):
        logger.info(
            f"Processing artist {artist_idx}/{total_artists}: {artist['name'][:20]}...",
            extra=extra
        )

        for track in artist['tracks']:
            try:
                if any(field in track for field in TARGET_FIELDS):
                    stats['skipped'] += 1
                    if stats['skipped'] % 50 == 0:
                        logger.debug(
                            f"Skipped {stats['skipped']} tracks",
                            extra=extra
                        )
                    continue

                clean_name = track['name'].split(' (', 1)[0].strip()
                query = f"{artist['name']} {clean_name}"

                song_data = client.get_song_info(query)

                if song_data:
                    track.update({
                        "language": song_data.get("language", "N/A"),
                        "release_date": song_data.get("release_date", "N/A"),
                        "album_name": (song_data.get("album") or {}).get("name", "N/A"),
                        "producer_artists": ", ".join(
                            [a.get("name", "N/A") for a in song_data.get("producer_artists", [])]),
                        "writer_artists": ", ".join(
                            [a.get("name", "N/A") for a in song_data.get("writer_artists", [])]),
                        "featured_artists": ", ".join(
                            [a.get("name", "N/A") for a in song_data.get("featured_artists", [])])
                    })
                    stats['processed'] += 1
                    logger.info(
                        f"Updated {clean_name[:20]}... (Album: {track['album_name'][:20]}...)",
                        extra={'category': 'PROCESSING'}
                    )
                else:
                    stats['errors'] += 1
                    logger.warning(
                        f"Failed to process: {query[:40]}...",
                        extra={'category': 'PROCESSING'}
                    )

                if (stats['processed'] + stats['errors']) % 10 == 0:
                    progress = (stats['processed'] + stats['errors'] + stats['skipped']) / total_tracks * 100
                    logger.info(
                        f"Progress: {progress:.1f}% | "
                        f"Processed: {stats['processed']} | "
                        f"Errors: {stats['errors']} | "
                        f"Skipped: {stats['skipped']}",
                        extra=extra
                    )
                if stats['processed'] % 100 == 0 and stats['processed'] > 0:
                    save_data(artists)

            except Exception as e:
                stats['errors'] += 1
                logger.error(
                    f"Critical error: {str(e)[:200]}",
                    exc_info=True,
                    extra={'category': 'PROCESSING'}
                )

    save_data(artists)
except Exception as e:
    logger.critical(
        f"Fatal error: {str(e)}",
        exc_info=True,
        extra=extra
    )
finally:
    logger.info(
        f"Completed in {time.time() - start_time:.1f}s\n"
        f"Artists: {total_artists}\n"
        f"Tracks: {total_tracks}\n"
        f"Processed: {stats['processed']}\n"
        f"Skipped: {stats['skipped']}\n"
        f"Errors: {stats['errors']}",
        extra=extra
    )
