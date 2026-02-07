import configparser
import datetime as _dt
import gzip
import json
import logging
import os
import re
from html.parser import HTMLParser
from types import SimpleNamespace
from urllib.parse import parse_qs, urljoin, urldefrag, urlparse
from urllib.robotparser import RobotFileParser

from bs4 import BeautifulSoup
from utils.config import Config
from utils.download import download
from utils.server_registration import get_cache_server
from analytics import track_page

from bs4 import XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

_ROBOTS_CACHE = {}
_CACHE_SERVER = None


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
    # Track page for analytics (saved at end of crawl)
    track_page(url, text)
    store_document(url, text, resp=resp)
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
        robots_text = _fetch_robots_via_cache(robots_url)
        if robots_text is None:
            rp.allow_all = True
        else:
            rp.parse(robots_text.splitlines())
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


def _get_cache_server():
    global _CACHE_SERVER
    if _CACHE_SERVER:
        return _CACHE_SERVER
    config_path = os.path.join(os.path.dirname(__file__), "config.ini")
    parser = configparser.ConfigParser()
    parser.read(config_path)
    config = Config(parser)
    _CACHE_SERVER = get_cache_server(config, False)
    return _CACHE_SERVER


def _fetch_robots_via_cache(robots_url):
    cache_server = _get_cache_server()
    if not cache_server:
        return None
    temp_config = SimpleNamespace(
        cache_server=cache_server,
        user_agent=_get_user_agent(),
    )
    resp = download(robots_url, temp_config, logger=None)
    if not resp or resp.status != 200 or not resp.raw_response:
        return None
    return resp.raw_response.text


def retrieve_text(url, resp):
    """Extract HTML text from the response."""
    logger = logging.getLogger(__name__)
    if not resp or resp.status != 200:
        logger.debug("retrieve_text: non-200 or missing resp for %s", url)
        return None
    if not resp.raw_response:
        logger.debug("retrieve_text: missing raw_response for %s", url)
        return None

    headers = getattr(resp.raw_response, "headers", {}) or {}
    content_type = headers.get("Content-Type", "") or headers.get("content-type", "")
    mime_type = content_type.split(";", 1)[0].strip().lower()
    if mime_type:
        allowed = (
            mime_type.startswith("text/") or
            mime_type in {"application/xhtml+xml", "application/xml", "text/xml"}
        )
        if not allowed:
            logger.debug("retrieve_text: rejected content-type %s for %s", mime_type, url)
            return None

    raw_bytes = getattr(resp.raw_response, "content", None)
    if not raw_bytes:
        logger.debug("retrieve_text: empty content for %s", url)
        return None

    charset_match = re.search(r"charset=([A-Za-z0-9_\-]+)", content_type, re.IGNORECASE)
    encoding = charset_match.group(1) if charset_match else None
    if not encoding:
        encoding = getattr(resp.raw_response, "encoding", None)
    if not encoding:
        encoding = "utf-8"

    try:
        page_text = raw_bytes.decode(encoding, errors="replace")
    except Exception:
        page_text = raw_bytes.decode("utf-8", errors="replace")

    if not page_text or len(page_text.strip()) < 20:
        logger.debug("retrieve_text: too short/blank for %s", url)
        return None
    return page_text


def store_document(url, text, resp=None, source=None, base_dir="./data/raw", max_bytes=100 * 1024 * 1024):
    """Store a single scraped item as JSONL (gzipped) with rotation."""
    if not url or text is None:
        return None
    if source is None:
        parsed = urlparse(url)
        source = parsed.netloc or "unknown"
    status = getattr(resp, "status", None)
    record = {
        "url": url,
        "fetched_at": _dt.datetime.utcnow().isoformat() + "Z",
        "status": status if isinstance(status, int) else None,
        "source": source,
        "data": {"text": text},
    }
    return store_records([record], base_dir=base_dir, max_bytes=max_bytes)


def store_records(records, base_dir="./data/raw", max_bytes=100 * 1024 * 1024):
    """Append records to a .jsonl.gz file with rotation by size."""
    if not records:
        return None
    date_str = _dt.datetime.utcnow().date().isoformat()
    source = records[0].get("source", "unknown")
    target_dir = os.path.join(base_dir, f"source={source}", f"dt={date_str}")
    os.makedirs(target_dir, exist_ok=True)

    part_idx = _next_part_index(target_dir, max_bytes)
    filename = f"part-{part_idx:05d}.jsonl.gz"
    file_path = os.path.join(target_dir, filename)

    with gzip.open(file_path, "ab") as gz:
        for record in records:
            normalized = _normalize_record(record)
            line = json.dumps(normalized, ensure_ascii=False)
            gz.write((line + "\n").encode("utf-8"))
    return file_path


def _normalize_record(record):
    required = {
        "url": "",
        "fetched_at": _dt.datetime.utcnow().isoformat() + "Z",
        "status": None,
        "source": "unknown",
        "data": {},
    }
    normalized = dict(required)
    normalized.update(record or {})
    return normalized


def _next_part_index(target_dir, max_bytes):
    max_idx = -1
    current_path = None
    for name in os.listdir(target_dir):
        if not name.startswith("part-") or not name.endswith(".jsonl.gz"):
            continue
        try:
            idx = int(name[len("part-"):len("part-") + 5])
        except ValueError:
            continue
        if idx > max_idx:
            max_idx = idx
            current_path = os.path.join(target_dir, name)

    if max_idx == -1:
        return 0
    if current_path and os.path.getsize(current_path) >= max_bytes:
        return max_idx + 1
    return max_idx


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
        # Convert to absolute URL and defragment (remove #fragment)
        try:
            absolute_url = urljoin(base_url, cleaned)
            defragged_url, _ = urldefrag(absolute_url)
            absolute_links.append(defragged_url)
        except:
            pass
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
        if is_trap(url):
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



def is_trap(url: str) -> bool:
    parsed = urlparse(url)
    path_lower = (parsed.path or "").lower()

    # Overly long or deeply nested URLs
    if len(url) > 2000:
        return True
    if len([seg for seg in parsed.path.split("/") if seg]) > 10:
        return True

    # Calendar traps and suspicious dates
    current_year = _dt.datetime.utcnow().year
    date_patterns = [
        r"/(19|20)\d{2}/\d{1,2}/\d{1,2}/",  # /YYYY/MM/DD/
        r"/(19|20)\d{2}-\d{1,2}-\d{1,2}/",  # /YYYY-MM-DD/
        r"[?&](date|day|month|year)=\d{4}[-/]\d{1,2}[-/]\d{1,2}",  # ?date=YYYY-MM-DD
    ]
    for pattern in date_patterns:
        for match in re.finditer(pattern, url):
            year_match = re.search(r"(19|20)\d{2}", match.group(0))
            if year_match:
                year_val = int(year_match.group(0))
                if abs(year_val - current_year) > 3:
                    return True

    # Repeated query parameters or pagination loops
    query = parse_qs(parsed.query, keep_blank_values=True)
    for key, values in query.items():
        if len(values) > 1:
            return True
    for key in query.keys():
        key_lower = key.lower()
        if key_lower in {"page", "p", "start", "offset", "paged"}:
            if len(query[key]) > 1:
                return True
        if key_lower in {"ical", "outlook-ical", "icalendar", "format"}:
            return True

    # Session or tracking IDs
    tracking_keys = {
        "sessionid", "sid", "phpsessid", "jsessionid", "ref",
        "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
        "gclid", "fbclid",
    }
    for key in query.keys():
        if key.lower() in tracking_keys:
            return True

    # Calendar archive traps (daily pages)
    if re.search(r"/events/\d{4}-\d{2}-\d{2}$", path_lower):
        return True
    if re.search(r"/events/.*/day/\d{4}-\d{2}-\d{2}", path_lower):
        return True
    if re.search(r"/calendar/|/events/[^/]+/day/\d{4}-\d{2}-\d{2}", path_lower):
        return True
    if re.search(r"/events/today/?$", path_lower):
        return True

    # Calendar archive traps (month/list/tag views)
    if re.search(r"/events/month(/|$)", path_lower):
        return True
    if re.search(r"/events/month/\d{4}-\d{2}", path_lower):
        return True
    if re.search(r"/events/list(/|$)", path_lower):
        return True
    if re.search(r"/events/list/page/\d+(/|$)", path_lower):
        return True
    if re.search(r"/events/tag/[^/]+/\d{4}-\d{2}$", path_lower):
        return True
    if re.search(r"/events/tag/[^/]+/list(/|$)", path_lower):
        return True
    if re.search(r"/events/tag/[^/]+/list/page/\d+(/|$)", path_lower):
        return True

    # Trap-like query parameter names common in calendars/feeds
    for key in query.keys():
        key_lower = key.lower()
        if re.search(r"(calendar|ical|feed|rss|atom)", key_lower):
            return True
        if key_lower in {"tribe-bar-date", "eventdisplay", "tribe_event", "eventdate"}:
            return True
        if "date" in key_lower or "event" in key_lower or "tribe" in key_lower:
            for value in query.get(key, []):
                if re.search(r"\d{4}-\d{2}-\d{2}", value):
                    return True

    # DokuWiki navigation/revision traps and media endpoints
    if "/doku.php" in path_lower:
        doku_query_keys = {k.lower() for k in query.keys()}
        doku_trap_keys = {
            "do", "idx", "rev", "rev2", "difftype", "sectok",
            "tab_files", "tab_details",
        }
        if doku_query_keys & doku_trap_keys:
            return True
        do_values = [v.lower() for v in query.get("do", [])]
        doku_trap_do_values = {
            "edit", "index", "recent", "backlink", "diff", "revisions", "media"
        }
        if any(v in doku_trap_do_values for v in do_values):
            return True
    if re.search(r"/lib/exe/(fetch|detail)\.php", path_lower):
        return True
    media_query_keys = {"image", "media", "tab_files", "tab_details", "sectok"}
    if any(k.lower() in media_query_keys for k in query.keys()):
        return True
    image_exts = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp", ".ico", ".tiff")
    for values in query.values():
        for value in values:
            if value.lower().endswith(image_exts):
                return True

    # Excessively large numeric pagination values
    for key, values in query.items():
        if key.lower() in {"page", "p", "start", "offset"}:
            for value in values:
                if value.isdigit() and int(value) > 1000:
                    return True

    return False
