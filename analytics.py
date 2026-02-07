import json
import os
import re
from collections import Counter
from threading import Lock
from urllib.parse import urlparse

from bs4 import BeautifulSoup


class CrawlerAnalytics:
    """Thread-safe analytics tracker for the web crawler."""

    def __init__(self, save_file="analytics_data.json"):
        self.save_file = save_file
        self.lock = Lock()

        self.unique_pages = 0
        self.word_counter = Counter()
        self.longest_page = {"url": "", "word_count": 0}
        self.subdomain_counter = Counter()

        self._load_data()
    
    def increment_page_count(self):
        with self.lock:
            self.unique_pages += 1

    def process_page(self, url, html_text):
        """
        Process a crawled page and update analytics.

        Args:
            url: The URL of the page
            html_text: The HTML content of the page
        """
        with self.lock:
            self.unique_pages += 1

            subdomain = self._extract_subdomain(url)
            if subdomain:
                self.subdomain_counter[subdomain] += 1

            text = self._extract_text_from_html(html_text)
            words = self._tokenize(text)
            word_count = len(words)

            if word_count > self.longest_page["word_count"]:
                self.longest_page = {"url": url, "word_count": word_count}

            filtered_words = self._filter_stopwords(words)
            self.word_counter.update(filtered_words)

    def _extract_subdomain(self, url):
        """Extract subdomain from URL if it's a uci.edu domain."""
        try:
            parsed = urlparse(url)
            netloc = parsed.netloc.lower()

            if netloc.endswith('.uci.edu'):
                return netloc

            return None
        except Exception:
            return None

    def _extract_text_from_html(self, html_text):
        """Extract visible text from HTML, removing markup."""
        try:
            soup = BeautifulSoup(html_text, 'lxml')
        except Exception:
            soup = BeautifulSoup(html_text, 'html.parser')

        # Remove script and style elements
        for script in soup(["script", "style", "meta", "link", "noscript"]):
            script.decompose()

        # Get text
        text = soup.get_text(separator=' ', strip=True)
        return text

    def _tokenize(self, text):
        """Tokenize text into words (alphanumeric sequences)."""
        # Extract words: sequences of alphanumeric characters
        words = re.findall(r'\b[a-z0-9]+\b', text.lower())
        return words

    def _filter_stopwords(self, words):
        """Remove common English stop words."""
        stopwords = {
            'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an',
            'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before',
            'being', 'below', 'between', 'both', 'but', 'by', 'can', 'could',
            'did', 'do', 'does', 'doing', 'down', 'during', 'each', 'few', 'for',
            'from', 'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here',
            'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in',
            'into', 'is', 'it', 'its', 'itself', 'just', 'me', 'might', 'more',
            'most', 'must', 'my', 'myself', 'no', 'nor', 'not', 'now', 'of', 'off',
            'on', 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out',
            'over', 'own', 's', 'same', 'she', 'should', 'so', 'some', 'such',
            't', 'than', 'that', 'the', 'their', 'theirs', 'them', 'themselves',
            'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to',
            'too', 'under', 'until', 'up', 'very', 'was', 'we', 'were', 'what',
            'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with',
            'would', 'you', 'your', 'yours', 'yourself', 'yourselves'
        }
        return [w for w in words if w not in stopwords and len(w) > 1]

    def save(self):
        """Save analytics data to disk."""
        with self.lock:
            data = {
                "unique_pages": self.unique_pages,
                "longest_page": self.longest_page,
                "top_50_words": self.word_counter.most_common(50),
                "subdomain_counts": dict(self.subdomain_counter)
            }

            with open(self.save_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

    def _load_data(self):
        """Load analytics data from disk if it exists."""
        if not os.path.exists(self.save_file):
            return

        try:
            with open(self.save_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.unique_pages = data.get("unique_pages", 0)
            self.longest_page = data.get("longest_page", {"url": "", "word_count": 0})

            # Restore word counter
            top_words = data.get("top_50_words", [])
            self.word_counter = Counter(dict(top_words))

            # Restore subdomain counter
            subdomain_data = data.get("subdomain_counts", {})
            self.subdomain_counter = Counter(subdomain_data)
        except Exception as e:
            print(f"Warning: Could not load analytics data: {e}")

    def get_report(self):
        """Generate a formatted report string."""
        with self.lock:
            lines = []
            lines.append("=" * 70)
            lines.append("WEB CRAWLER ANALYTICS REPORT")
            lines.append("=" * 70)
            lines.append("")

            # 1. Unique pages
            lines.append(f"1. Unique Pages Found: {self.unique_pages}")
            lines.append("")

            # 2. Longest page
            lines.append(f"2. Longest Page (by word count):")
            lines.append(f"   URL: {self.longest_page['url']}")
            lines.append(f"   Word Count: {self.longest_page['word_count']:,}")
            lines.append("")

            # 3. Top 50 common words
            lines.append("3. 50 Most Common Words (excluding stop words):")
            for i, (word, count) in enumerate(self.word_counter.most_common(50), 1):
                lines.append(f"   {i:2d}. {word:20s} - {count:,}")
            lines.append("")

            # 4. Subdomains
            lines.append("4. Subdomains in *.uci.edu (alphabetically sorted):")
            sorted_subdomains = sorted(self.subdomain_counter.items())
            for subdomain, count in sorted_subdomains:
                lines.append(f"   {subdomain}, {count}")
            lines.append("")
            lines.append(f"   Total subdomains: {len(self.subdomain_counter)}")
            lines.append("")

            lines.append("=" * 70)

            return "\n".join(lines)

    def print_report(self):
        """Print the analytics report to console."""
        print(self.get_report())

    def save_report(self, filename="REPORT.txt"):
        """Save the report to a text file."""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(self.get_report())


# Global analytics instance (singleton pattern)
_analytics_instance = None
_analytics_lock = Lock()


def get_analytics():
    """Get the global analytics instance (thread-safe singleton)."""
    global _analytics_instance
    if _analytics_instance is None:
        with _analytics_lock:
            if _analytics_instance is None:
                _analytics_instance = CrawlerAnalytics()
    return _analytics_instance


def track_page(url, html_text):
    """Convenience function to track a page."""
    analytics = get_analytics()
    analytics.process_page(url, html_text)


def save_analytics():
    """Convenience function to save analytics data."""
    analytics = get_analytics()
    analytics.save()


# save analytics every N pages
_fetch_count = 0
_fetch_lock = Lock()
_FETCH_THRESHOLD = 50


def notify_fetch(increment: int = 1):
    """Notify the analytics subsystem that N pages have been fetched.
    """
    global _fetch_count
    with _fetch_lock:
        _fetch_count += increment
        if _fetch_count % _FETCH_THRESHOLD == 0:
            save_analytics()


def generate_report():
    """Convenience function to generate and save the report."""
    analytics = get_analytics()
    analytics.print_report()
    analytics.save_report()
    analytics.save()
