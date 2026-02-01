import re
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

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
    # TODO: implement policy checks (status, content-type, robots, etc.).
    return True


def retrieve_text(url, resp):
    """Extract HTML text from the response."""
    # TODO: extract text safely from resp.raw_response.content.
    # Placeholder returns None to avoid processing until implemented.
    return None


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

    class _LinkParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = []
        def handle_starttag(self, tag, attrs):
            if tag.lower() != "a":
                return
            for name, value in attrs:
                if name.lower() == "href" and value:
                    self.links.append(value)

    parser = _LinkParser()
    try:
        parser.feed(text)
    except Exception:
        return []

    absolute_links = []
    for href in parser.links:
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
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
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
        print ("TypeError for ", parsed)
        raise
