import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
from tqdm import tqdm
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spotify_data.log'),
        logging.StreamHandler()
    ]
)

client_id = "ffc4693b77064fa2bce53f34878011e4"
client_secret = "7af08178a1ee421c92ea62e5680a55cf"

logging.info("Инициализация Spotify клиента")
start_time = time.time()

client_credentials_manager = SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
)

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

logging.info(f"Клиент инициализирован за {time.time() - start_time:.2f} секунд")


def get_popular_artists(limit=1000):
    artists = []
    artist_ids = set()

    genres = [
        'pop', 'rock', 'hip hop', 'electronic', 'jazz',
        'classical', 'reggae', 'country', 'r&b', 'metal',
        'indie', 'blues', 'soul', 'funk', 'k-pop', 'latin',
        'folk', 'punk', 'techno', 'house', 'disco', 'dance',
        'alternative', 'gospel', 'world', 'soundtrack',
        'ambient', 'reggaeton', 'trap', 'edm', 'dubstep',
        'synthwave', 'emo', 'grunge', 'hardcore', 'progressive'
    ]

    countries = ['US', 'GB', 'DE', 'JP', 'KR', 'BR', 'FR', 'ES', 'IT']

    logging.info(f"Начало поиска артистов по {len(genres)} жанрам и {len(countries)} странам")

    genre_artists = 0
    for genre in tqdm(genres, desc="Обработка жанров"):
        try:
            results = sp.search(
                q=f'genre:"{genre}"',
                type='artist',
                limit=50
            )
            found = len(results['artists']['items'])
            genre_artists += found
            logging.debug(f"Жанр {genre}: найдено {found} артистов")

            for artist in results['artists']['items']:
                if artist['id'] not in artist_ids:
                    artists.append({
                        'id': artist['id'],
                        'name': artist['name'],
                        'genre': genre,
                        'popularity': artist['popularity']
                    })
                    artist_ids.add(artist['id'])

        except Exception as e:
            logging.error(f"Ошибка в жанре {genre}: {str(e)}", exc_info=True)

    logging.info(f"По жанрам найдено {genre_artists} уникальных артистов")

    country_artists = 0
    for country in tqdm(countries, desc="Чарты стран"):
        try:
            results = sp.category_playlists(
                category_id='toplists',
                country=country,
                limit=50
            )
            playlist_count = len(results['playlists']['items'])
            logging.debug(f"Страна {country}: найдено {playlist_count} плейлистов")

            for playlist in results['playlists']['items']:
                try:
                    tracks = sp.playlist_tracks(playlist['id'], limit=50)['items']
                    for item in tracks:
                        artist = item['track']['artists'][0]
                        if artist['id'] not in artist_ids:
                            artists.append({
                                'id': artist['id'],
                                'name': artist['name'],
                                'genre': 'chart',
                                'popularity': 70
                            })
                            artist_ids.add(artist['id'])
                            country_artists += 1
                except Exception as e:
                    logging.error(f"Ошибка в плейлисте {playlist['id']}: {str(e)}")

        except Exception as e:
            logging.error(f"Ошибка в стране {country}: {str(e)}", exc_info=True)

    logging.info(f"По странам найдено {country_artists} уникальных артистов")
    logging.info(f"Всего найдено {len(artists)} артистов до применения лимита")

    return artists[:limit]


def get_artist_top_tracks(artist_id):
    try:
        logging.debug(f"Начало обработки артиста {artist_id}")
        start_time = time.time()

        tracks = []
        top_tracks = sp.artist_top_tracks(artist_id, country='US')['tracks']
        albums = sp.artist_albums(artist_id, album_type='album,single', limit=100)['items']

        tracks.extend({
                          'id': track['id'],
                          'name': track['name'],
                          'duration_ms': track['duration_ms'],
                          'popularity': track['popularity']
                      } for track in top_tracks)

        album_track_count = 0
        for album in albums:
            try:
                album_tracks = sp.album_tracks(album['id'])['items']
                for track in album_tracks:
                    if track['id'] not in {t['id'] for t in tracks}:
                        tracks.append({
                            'id': track['id'],
                            'name': track['name'],
                            'duration_ms': track['duration_ms'],
                            'popularity': track.get('popularity', 70)
                        })
                        album_track_count += 1
            except Exception as e:
                logging.error(f"Ошибка при обработке альбома {album['id']}: {str(e)}")

        logging.debug(f"Артист {artist_id} обработан за {time.time() - start_time:.2f} сек. "
                      f"Треков: {len(tracks)} (топ: {len(top_tracks)}, альбомы: {album_track_count})")

        return tracks

    except Exception as e:
        logging.error(f"Критическая ошибка для артиста {artist_id}: {str(e)}", exc_info=True)
        return []


try:
    logging.info("Запуск процесса сбора данных")
    total_start = time.time()

    artists = get_popular_artists(1000)
    logging.info(f"Получено {len(artists)} артистов для обработки")

    artists_data = []
    total_tracks = 0
    save_interval = 100
    errors_count = 0

    for i, artist in tqdm(enumerate(artists), total=len(artists), desc="Сбор треков"):
        try:
            tracks = get_artist_top_tracks(artist['id'])
            track_count = len(tracks)
            artist['tracks'] = tracks
            artists_data.append(artist)
            total_tracks += track_count

            if (total_tracks // save_interval) > ((total_tracks - track_count) // save_interval):
                logging.info(f"Промежуточное сохранение: {total_tracks} треков")
                with open('artists_with_tracks.json', 'w', encoding='utf-8') as f:
                    json.dump(artists_data, f, indent=4, ensure_ascii=False)

        except Exception as e:
            errors_count += 1
            logging.error(f"Ошибка обработки артиста {artist['id']}: {str(e)}")

    logging.info("Финальное сохранение данных")
    with open('artists_with_tracks.json', 'w', encoding='utf-8') as f:
        json.dump(artists_data, f, indent=4, ensure_ascii=False)

    total_time = time.time() - total_start
    avg_tracks = total_tracks / len(artists_data) if artists_data else 0
    logging.info(
        f"\nИтоговая статистика:\n"
        f"Артистов: {len(artists_data)}\n"
        f"Треков: {total_tracks}\n"
        f"Ошибок: {errors_count}\n"
        f"Время выполнения: {total_time // 3600:.0f} ч {total_time % 3600 // 60:.0f} м {total_time % 60:.2f} с\n"
        f"Среднее треков на артиста: {avg_tracks:.1f}"
    )

except Exception as e:
    logging.critical(f"Критическая ошибка в основном потоке: {str(e)}", exc_info=True)
finally:
    logging.info("Завершение работы скрипта")
