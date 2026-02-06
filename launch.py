from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
from analytics import save_analytics, generate_report
import multiprocessing
import sys


if sys.platform == "darwin":
    try:
        multiprocessing.set_start_method("fork", force=True)
    except RuntimeError:
        pass


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()

    print("\n" + "="*70)
    print("Crawling completed! Saving final analytics...")
    save_analytics()
    print("Generating final report...")
    generate_report()
    print("="*70)
    print("\nReport saved to REPORT.txt")
    print("Analytics data saved to analytics_data.json")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
