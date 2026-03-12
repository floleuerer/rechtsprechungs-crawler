import os
import time
import json
import re
import random
from urllib.parse import urlparse, urljoin, urlencode, parse_qs, urlunparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


class NiedersachsenCrawler:
    """
    Crawler für Niedersachsen — Rechtsprechungsdatenbank NI-VORIS
    Portal: https://voris.wolterskluwer-online.de
    Targets Rechtsprechung (case law) documents.
    """

    BASE_URL = "https://voris.wolterskluwer-online.de"
    # Filter URL for Rechtsprechung category
    SEARCH_URL = "https://voris.wolterskluwer-online.de/search?query=&publicationtype=publicationform-ats-filter%21ATS_Rechtsprechung"

    DEFAULT_USER_AGENT = "LegalCrawl/0.1 (legal research crawler)"
    STEALTH_USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]

    def __init__(self, output_dir="data/niedersachsen_raw", headless=True, limit=None,
                 overwrite=False, skip_threshold=50, stealth=False):
        self.output_dir = output_dir
        self.headless = headless
        self.limit = limit
        self.overwrite = overwrite
        self.skip_threshold = skip_threshold
        self.stealth = stealth
        self.driver = None
        self.crawled_count = 0
        self.consecutive_skips = 0

        os.makedirs(self.output_dir, exist_ok=True)

    def setup_driver(self):
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        ua = random.choice(self.STEALTH_USER_AGENTS) if self.stealth else self.DEFAULT_USER_AGENT
        options.add_argument(f"user-agent={ua}")

        self.driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=options
        )

    def random_sleep(self, min_sec=1.5, max_sec=3.0):
        time.sleep(random.uniform(min_sec, max_sec))

    def _page_url(self, page_num):
        """Build search URL for a given page number (0-indexed)."""
        if page_num == 0:
            return self.SEARCH_URL
        return f"{self.SEARCH_URL}&page={page_num}"

    def _uuid_from_url(self, url):
        """Extract UUID from /browse/document/[UUID] URLs."""
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        # Expected: ['browse', 'document', '<uuid>']
        if len(parts) >= 3 and parts[1] == "document":
            return parts[2]
        if parts:
            return parts[-1]
        return None

    def _wait_for_results(self, wait):
        """Wait for the search result list to appear."""
        # The portal renders a list; we look for any anchor pointing to /browse/document/
        try:
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a[href*='/browse/document/']")
                )
            )
            return True
        except TimeoutException:
            return False

    def _get_result_links(self):
        """Return all document links on the current search result page."""
        elements = self.driver.find_elements(
            By.CSS_SELECTOR, "a[href*='/browse/document/']"
        )
        # Deduplicate while preserving order
        seen = set()
        links = []
        for el in elements:
            href = el.get_attribute("href") or ""
            uuid = self._uuid_from_url(href)
            if uuid and uuid not in seen:
                seen.add(uuid)
                links.append(href)
        return links

    def _has_next_page(self):
        """Check if a 'next page' link exists and is not disabled."""
        # VORIS uses an anchor with text/aria 'Zur nächsten Seite' or rel='next'
        candidates = self.driver.find_elements(
            By.XPATH,
            "//a[contains(@aria-label,'nächste') or contains(text(),'nächste') or @rel='next']"
        )
        for el in candidates:
            classes = el.get_attribute("class") or ""
            if "disabled" not in classes and el.is_displayed():
                return True
        return False

    def crawl(self):
        self.setup_driver()
        try:
            page_num = 0
            while True:
                url = self._page_url(page_num)
                print(f"--- Page {page_num + 1}: {url} ---")
                self.driver.get(url)

                wait = WebDriverWait(self.driver, 20)

                if not self._wait_for_results(wait):
                    print("No results found or page timed out. Stopping.")
                    break

                self.random_sleep(1, 2)

                links = self._get_result_links()
                print(f"Found {len(links)} document links on page {page_num + 1}.")

                if not links:
                    print("No document links found. Stopping.")
                    break

                for doc_url in links:
                    if self.limit and self.crawled_count >= self.limit:
                        print(f"Limit of {self.limit} reached.")
                        return

                    if self.skip_threshold and self.consecutive_skips >= self.skip_threshold:
                        print(f"Reached {self.skip_threshold} consecutive existing files. Stopping.")
                        return

                    uuid = self._uuid_from_url(doc_url)
                    if not uuid:
                        continue

                    safe_uuid = "".join(c for c in uuid if c.isalnum() or c in ("-", "_", "."))
                    filename = f"{safe_uuid}.json"
                    filepath = os.path.join(self.output_dir, filename)

                    if not self.overwrite and os.path.exists(filepath):
                        print(f"  Skipping existing: {filename}")
                        self.consecutive_skips += 1
                        continue

                    # Open in new tab to preserve search results page
                    self.driver.execute_script("window.open(arguments[0], '_blank');", doc_url)
                    self.driver.switch_to.window(self.driver.window_handles[-1])

                    try:
                        self._process_document(doc_url, filepath, wait)
                    except Exception as e:
                        print(f"  Error processing {doc_url}: {e}")
                    finally:
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])

                    self.random_sleep(0.5, 1.5)

                # Pagination: check for next page
                if not self._has_next_page():
                    print("No next page found. Crawling complete.")
                    break

                page_num += 1
                self.random_sleep(1.5, 3.0)

        finally:
            if self.driver:
                self.driver.quit()

    def _wait_for_document(self, wait):
        """Wait for document content to load on detail page."""
        # Try multiple content selectors in order of specificity
        selectors = [
            "main",
            "article",
            ".document-content",
            ".doc-content",
            ".content",
            "#content",
            "[class*='document']",
            "[class*='content']",
        ]
        for selector in selectors:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                return selector
            except TimeoutException:
                continue
        return None

    def _process_document(self, url, filepath, wait):
        """Load a document page, extract data, and save as JSON."""
        # Wait for page to load
        content_selector = self._wait_for_document(wait)
        if not content_selector:
            print(f"  Warning: Could not detect content container for {url}")

        self.random_sleep(0.5, 1.0)

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        data = {
            "url": url,
            "crawled_at": time.time(),
            "metadata": {},
            "content": "",
            "html_content": "",
        }

        # --- Metadata extraction ---
        # Title from h1
        h1 = soup.select_one("h1")
        if h1:
            data["metadata"]["title"] = h1.get_text(strip=True)

        # Generic key:value metadata from definition lists or tables
        for dl in soup.select("dl"):
            dts = dl.select("dt")
            dds = dl.select("dd")
            for dt, dd in zip(dts, dds):
                key = dt.get_text(strip=True).rstrip(":")
                val = dd.get_text(strip=True)
                if key:
                    data["metadata"][key] = val

        for row in soup.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if th and td:
                key = th.get_text(strip=True).rstrip(":")
                val = td.get_text(strip=True)
                if key:
                    data["metadata"][key] = val

        # Also try labeled/value div pairs common in Wolters Kluwer portals
        for label_el in soup.select("[class*='label'], [class*='Label']"):
            label_text = label_el.get_text(strip=True).rstrip(":")
            value_el = label_el.find_next_sibling()
            if value_el and label_text:
                data["metadata"][label_text] = value_el.get_text(strip=True)

        # VORIS number (prominent metadata field)
        voris_pattern = re.compile(r'VORIS\s*(?:Nr\.?|Nummer|:)?\s*([\d\s]+)', re.IGNORECASE)
        page_text = soup.get_text()
        voris_match = voris_pattern.search(page_text)
        if voris_match and "voris_nummer" not in data["metadata"]:
            data["metadata"]["voris_nummer"] = voris_match.group(1).strip()

        # --- Content extraction ---
        # Priority order: main > article > largest div with meaningful text
        content_elem = (
            soup.select_one("main")
            or soup.select_one("article")
            or soup.select_one(".document-content, .doc-content, #document-content")
            or soup.select_one("[class*='document'][class*='content']")
            or soup.select_one("[class*='docbody'], [class*='doc-body']")
        )

        if not content_elem:
            # Fallback: find the div with the most text
            divs = soup.find_all("div")
            if divs:
                content_elem = max(divs, key=lambda d: len(d.get_text()))

        if content_elem:
            # Remove navigation/header noise
            for noise in content_elem.select("nav, header, footer, script, style, [class*='nav'], [class*='sidebar']"):
                noise.decompose()
            data["content"] = content_elem.get_text(separator="\n", strip=True)
            data["html_content"] = str(content_elem)
        else:
            print(f"  Warning: No content container found for {url}")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        self.crawled_count += 1
        self.consecutive_skips = 0
        print(f"  Saved {os.path.basename(filepath)}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Niedersachsen Rechtsprechungs-Crawler (Portal: voris.wolterskluwer-online.de)"
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of documents to crawl")
    parser.add_argument("--no-headless", action="store_true",
                        help="Show browser window (useful for debugging)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing files")
    parser.add_argument("--stealth", action="store_true",
                        help="Use browser-like User-Agent instead of crawler UA")
    parser.add_argument("--skip-threshold", type=int, default=50,
                        help="Stop after N consecutive existing files (0 = disable)")
    parser.add_argument("--output", type=str, default="data/niedersachsen",
                        help="Output directory for JSON files")

    args = parser.parse_args()

    crawler = NiedersachsenCrawler(
        output_dir=args.output,
        headless=not args.no_headless,
        limit=args.limit,
        overwrite=args.overwrite,
        skip_threshold=args.skip_threshold,
        stealth=args.stealth,
    )
    crawler.crawl()
