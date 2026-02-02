import configparser
import os
import re
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

from bs4 import BeautifulSoup

_ROBOTS_CACHE = {}


def _load_user_agent():
    config_path = os.path.join(os.path.dirname(__file__), "config.ini")
    parser = configparser.ConfigParser()
    try:
        parser.read(config_path)
        return parser["IDENTIFICATION"]["USERAGENT"].strip()
    except Exception:
        return "*"


_USER_AGENT = _load_user_agent()


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    # Main scraper frame: check policy, extract text, store, parse links.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    return crawl_document(url, resp)


def crawl_document(url, resp):
    """Main frame logic from the pseudocode."""
    if not permits_crawl(url, resp):
        return []
    text = retrieve_text(url, resp)
    if text is None:
        return []
    store_document(url, text)
    return parse_text_for_links(url, text)


def permits_crawl(url, resp):
    """Decide whether the retrieved URL should be processed."""
    user_agent = _get_user_agent()
    rp = _get_robot_parser(url)
    if rp is None:
        return True

    if rp.disallow_all:
        return False
    elif rp.allow_all:
        return True
    elif _has_agent_rule(rp, user_agent):
        return rp.can_fetch(user_agent, url)
    elif _has_agent_rule(rp, "*"):
        return rp.can_fetch("*", url)
    else:
        return True


def _get_user_agent():
    return _USER_AGENT


def _get_robot_parser(url):
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return None
    key = f"{parsed.scheme}://{parsed.netloc}"
    if key in _ROBOTS_CACHE:
        return _ROBOTS_CACHE[key]

    robots_url = f"{key}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        rp.allow_all = True
    _ROBOTS_CACHE[key] = rp
    return rp


def _has_agent_rule(rp, agent):
    agent_lower = agent.lower()
    for entry in getattr(rp, "entries", []):
        for ua in entry.useragents:
            if ua.lower() == agent_lower:
                return True
    return False


def retrieve_text(url, resp):
    """Extract HTML text from the response."""
    # TODO: extract text safely from resp.raw_response.content.
    # Placeholder returns None to avoid processing until implemented.
    #make sure response is ok first
    if not resp or resp.status != 200:
        return None
    #make sure raw response exist
    if not resp.raw_response:
        return None

    page_text = resp.raw_response.text
    if not page_text or len(page_text.strip()) == 0:
        return None
    return page_text


def store_document(url, text):
    """Store document text or analytics artifacts."""
    # TODO: persist text or statistics for analysis/extra credit.
    return None


def parse_text_for_links(base_url, text):
    """Parse outgoing links from page text."""
    if text is None:
        return []
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8", errors="replace")
        except Exception:
            return []

    try:
        soup = BeautifulSoup(text, "lxml")
    except Exception:
        soup = BeautifulSoup(text, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a") if a.get("href")]

    absolute_links = []
    for href in hrefs:
        cleaned = href.strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered.startswith(("mailto:", "javascript:", "tel:", "data:")):
            continue
        if cleaned.startswith("#"):
            continue
        absolute_links.append(urljoin(base_url, cleaned))
    return absolute_links


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    allowed_domains = r".*\.(ics|cs|informatics|stat)\.uci\.edu$"
    try:
        parsed = urlparse(url)
        # make sure its http or https links
        if parsed.scheme not in set(["http", "https"]):
            return False
        if not re.match(allowed_domains, parsed.netloc.lower()):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise
