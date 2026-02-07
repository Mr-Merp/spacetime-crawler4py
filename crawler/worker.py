from threading import Thread, Lock
from inspect import getsource
from urllib.parse import urlparse
import time

from utils.download import download
from utils import get_logger
import scraper
from scraper import retrieve_text, store_document
from similarity import SimilarityTracker
import analytics

# Global variables shared by all worker threads to ensure cross-thread politeness
domain_times = {}
time_lock = Lock()


def get_polite(url, config):
    """
    Ensures that no single domain is hit more than once per config.time_delay.
    """
    domain = urlparse(url).netloc
    with time_lock:
        now = time.time()
        last_visit = domain_times.get(domain, 0)
        # config.time_delay is typically 0.5s from config.ini
        sleep_time = max(0, (last_visit + config.time_delay) - now)

        time.sleep(sleep_time)

        # Update the timestamp AFTER the sleep so the next thread knows to wait
        domain_times[domain] = time.time()


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        # Initialize the SimilarityTracker for near-duplicate detection
        # Note: In this version, each worker has its own tracker.
        self.similarity_tracker = SimilarityTracker(exact_threshold=1.0, near_threshold=0.88)

        # Safety checks to ensure 'requests' or 'urllib' aren't used in scraper.py
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {
            -1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {
            -1}, "Do not use urllib.request in scraper.py"

        super().__init__(daemon=True)

    def run(self):
        while True:
            # Get the next URL from the thread-safe Frontier
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            # 1. Respect Politeness PER DOMAIN before downloading
            get_polite(tbd_url, self.config)

            # 2. Download the page content from the cache server
            resp = download(tbd_url, self.config, self.logger)

            # 3. Extract text and check for exact/near duplicates
            text = retrieve_text(tbd_url, resp)
            is_dup, detection_method = self.similarity_tracker.is_similar(tbd_url, text)

            if text and not is_dup:
                # Page is new and unique: update analytics and store it
                store_document(tbd_url, text, resp=resp)

                # Extract out-links and add valid ones back to the Frontier
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)

                self.logger.info(f"Stored unique page: {tbd_url}, status <{resp.status}>, "
                                 f"using cache {self.config.cache_server}.")
                try:
                    analytics.notify_fetch()
                except Exception:
                    pass
            else:
                # Skip duplicate content or empty responses
                reason = detection_method if is_dup else "No content"
                self.logger.info(f"Skipping {tbd_url} - Reason: {reason}")

            # 4. Mark the URL as processed in the Frontier
            self.frontier.mark_url_complete(tbd_url)
