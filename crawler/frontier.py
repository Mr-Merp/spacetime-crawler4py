import os
import shelve
import sqlite3
from threading import RLock

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

_original_connect = sqlite3.connect


def _patched_connect(*args, **kwargs):
    kwargs['check_same_thread'] = False
    return _original_connect(*args, **kwargs)


sqlite3.connect = _patched_connect


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.lock = RLock()

        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)

        with self.lock:
            if os.path.exists(self.config.save_file) and restart:
                self.logger.info(f"Restarting: deleting {self.config.save_file}")
                # Logic to clear shelf if restarting
                self.save.clear()

            if restart or not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)
            else:
                self._parse_save_file()

    def _parse_save_file(self):
        with self.lock:
            total_count = len(self.save)
            tbd_count = 0
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    self.to_be_downloaded.append(url)
                    tbd_count += 1
            self.logger.info(f"Found {tbd_count} URLs to download from {total_count} total.")

    def get_tbd_url(self):
        with self.lock:
            try:
                return self.to_be_downloaded.pop(0)
            except IndexError:
                return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.append(url)

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.logger.error(f"Completed unknown URL: {url}")
            self.save[urlhash] = (url, True)
            self.save.sync()
