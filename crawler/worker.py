from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from scraper import retrieve_text, store_document
from similarity import SimilarityTracker


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        self.similarity_tracker = SimilarityTracker(exact_threshold=1.0, near_threshold=0.88)
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(...)
            
            # NEW: Extract text and check for duplicates
            text = retrieve_text(tbd_url, resp)
            is_dup, detection_method = self.similarity_tracker.is_similar(tbd_url, text)
            
            if text and not is_dup:
                # NEW page - store and extract links
                store_document(tbd_url, text, resp=resp)
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                self.logger.info(f"Stored new page: {tbd_url}")
            else:
                # DUPLICATE - skip storing
                self.logger.info(f"Skipped {detection_method} duplicate: {tbd_url}")
            
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
