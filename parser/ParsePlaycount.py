import json
import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import traceback


def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    log("Драйвер Chrome успешно инициализирован")
except Exception as e:
    log(f"Ошибка инициализации драйвера: {str(e)}", "ERROR")
    raise

if not os.path.exists('artists_with_tracks.json'):
    log("Файл artists_with_tracks.json не найден!", "CRITICAL")
    exit(1)

playcounts = {}
try:
    if os.path.exists('playcount.json'):
        with open('playcount.json', 'r', encoding='utf-8') as f:
            playcounts = json.load(f)
        log(f"Загружено {len(playcounts)} существующих записей из кэша")
except Exception as e:
    log(f"Ошибка загрузки playcount.json: {str(e)}", "WARNING")

try:
    with open('artists_with_tracks.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    log(f"Успешно загружено {len(data)} артистов из файла")
except Exception as e:
    log(f"Ошибка чтения artists_with_tracks.json: {str(e)}", "CRITICAL")
    exit(1)

track_ids = []
try:
    track_ids = [
        track["id"]
        for artist in data
        for track in artist.get("tracks", [])
    ]
    log(f"Собрано {len(track_ids)} треков для обработки")
except KeyError as e:
    log(f"Некорректная структура данных: отсутствует ключ {str(e)}", "ERROR")
    exit(1)

total = len(track_ids)
processed = 0
errors = 0
existing = len(playcounts)
start_time = time.time()

log(f"Старт обработки {total} треков. Уже в кэше: {existing}")

for i, track_id in enumerate(track_ids, 1):
    if track_id in playcounts:
        log(f"[{i}/{total}] Трек {track_id} уже в кэше, пропуск", "DEBUG")
        continue

    log(f"[{i}/{total}] Обработка трека {track_id}")
    url = f"https://open.spotify.com/track/{track_id}"

    try:
        driver.get(url)
        log(f"[{i}/{total}] Открыта страница трека", "DEBUG")
    except Exception as e:
        errors += 1
        log(f"[{i}/{total}] Ошибка загрузки страницы: {str(e)}", "ERROR")
        playcounts[track_id] = "LOAD_ERROR"
        continue

    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="playcount"]'))
        )
        raw_text = element.text.replace('\u00a0', ' ').replace(' ', '')

        if not raw_text.isdigit():
            raise ValueError(f"Некорректный формат числа: {raw_text}")

        playcount = int(raw_text)
        playcounts[track_id] = playcount
        log(f"[{i}/{total}] Успешно получено значение: {playcount}", "SUCCESS")

    except TimeoutException:
        errors += 1
        log(f"[{i}/{total}] Таймаут ожидания элемента", "WARNING")
        playcounts[track_id] = "TIMEOUT"

    except (NoSuchElementException, ValueError) as e:
        errors += 1
        log(f"[{i}/{total}] Ошибка извлечения данных: {str(e)}", "ERROR")
        playcounts[track_id] = "PARSE_ERROR"

    except Exception as e:
        errors += 1
        log(f"[{i}/{total}] Неожиданная ошибка: {traceback.format_exc()}", "ERROR")
        playcounts[track_id] = "UNKNOWN_ERROR"

    if i % 50 == 0:
        try:
            with open('playcount.json', 'w', encoding='utf-8') as f:
                json.dump(playcounts, f, ensure_ascii=False, indent=4)
            log(f"[{i}/{total}] Промежуточное сохранение ({i} треков)", "INFO")
        except Exception as e:
            log(f"Ошибка сохранения: {str(e)}", "CRITICAL")

try:
    driver.quit()
    log("Браузер успешно закрыт")
except Exception as e:
    log(f"Ошибка при закрытии браузера: {str(e)}", "WARNING")

try:
    with open('playcount.json', 'w', encoding='utf-8') as f:
        json.dump(playcounts, f, ensure_ascii=False, indent=4)
    log("Финальное сохранение выполнено успешно")
except Exception as e:
    log(f"Ошибка финального сохранения: {str(e)}", "CRITICAL")

execution_time = time.time() - start_time
success = len(playcounts) - errors - existing

log("\n=== ОТЧЕТ ===")
log(f"Всего треков: {total}")
log(f"Успешно обработано: {success}")
log(f"Из кэша: {existing}")
log(f"Ошибок обработки: {errors}")
log(f"Затраченное время: {execution_time:.2f} секунд")
log(f"Средняя скорость: {total / execution_time:.2f} треков/сек")
