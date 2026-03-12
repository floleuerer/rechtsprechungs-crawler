
import os
import time
import json
import argparse
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import requests.exceptions

def is_retryable_error(exception):
    """Check if the exception is one that we should retry on."""
    # Retry on specific HTTP status codes
    if isinstance(exception, requests.exceptions.HTTPError):
        if exception.response is not None and exception.response.status_code in [429, 500, 502, 503, 504]:
            return True
    
    # Retry on connection errors (including RemoteDisconnected)
    if isinstance(exception, (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError)):
        return True
        
    return False

class BayernCrawler:
    DEFAULT_USER_AGENT = "LegalCrawl/0.1 (legal research crawler)"

    def __init__(self, output_dir="data/bayern_raw", limit=None, overwrite=False, skip_threshold=50, stealth=False):
        self.output_dir = output_dir
        self.limit = limit
        self.overwrite = overwrite
        self.skip_threshold = skip_threshold
        self.stealth = stealth
        self.base_url = "https://www.gesetze-bayern.de"
        self.crawled_count = 0
        self.consecutive_skips = 0
        self.session = requests.Session()

        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" if self.stealth else self.DEFAULT_USER_AGENT
        self.session.headers.update({'User-Agent': ua})

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def random_sleep(self, min_sec=0.5, max_sec=1.5):
        time.sleep(random.uniform(min_sec, max_sec))

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(is_retryable_error)
    )
    def fetch_url(self, url):
        """Fetch URL with retry logic."""
        response = self.session.get(url)
        response.raise_for_status()
        return response

    def crawl(self):
        try:
            print(f"Initializing session...")
            # 1. Visit Homepage to initialize session/cookies
            self.fetch_url(self.base_url)
            self.random_sleep()

            # 2. Visit the "Rechtsprechung" filter URL to trigger the search list
            print("Accessing case law list...")
            start_url = f"{self.base_url}/Search/Filter/DOKTYP/rspr"
            resp = self.fetch_url(start_url)
            
            if resp.status_code != 200:
                print(f"Failed to load start page: {resp.status_code}")
                return

            self.process_hitlist(resp.text)

        except Exception as e:
            print(f"An error occurred during crawling: {e}")

    def process_hitlist(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        
        page_num = 1
        while True:
            print(f"--- Processing Result Page {page_num} ---")
            
            # Find result items
            # Selector identified: li.hitlistItem
            result_items = soup.select('li.hitlistItem')
            num_results = len(result_items)
            print(f"Found {num_results} results on this page.")
            
            if num_results == 0:
                print("No results found. Ending.")
                break

            for i, item in enumerate(result_items):
                if self.limit and self.crawled_count >= self.limit:
                    print(f"Limit of {self.limit} reached.")
                    return

                # Extract Link
                link_elem = item.select_one('p.hltitel a')
                if not link_elem:
                    continue
                
                href = link_elem.get('href')
                full_url = urljoin(self.base_url, href)
                
                # Extract basic info for skipping logic (optimization)
                # Attempt to extract Az from the subtitle or title to check purely based on filename if possible
                # But standard process defines we might need to fetch metadata. 
                # To save requests, we can check if we can guess the filename.
                # Title format in list: "<b>BayObLG: ..."
                # Subtitle format: "Beschluss vom 21.01.2026 – 102 Sch 78/25 e"
                subtitle_elem = item.select_one('p.hlSubTitel')
                skip = False
                if subtitle_elem and not self.overwrite:
                    subtitle_text = subtitle_elem.get_text(strip=True)
                    if "–" in subtitle_text or "-" in subtitle_text: # Watch out for different dash types
                         parts = subtitle_text.replace("–", "-").split("-")
                         if len(parts) > 1:
                             az_candidate = parts[-1].strip()
                             filename_check = f"{az_candidate.replace('/', '_').replace(' ', '_')}.json"
                             file_path_check = os.path.join(self.output_dir, filename_check)
                             if os.path.exists(file_path_check):
                                 print(f"  Skipping existing file (guessed): {filename_check}")
                                 self.consecutive_skips += 1
                                 if self.skip_threshold and self.consecutive_skips >= self.skip_threshold:
                                     print(f"Reached {self.skip_threshold} consecutive existing files. Stopping incremental crawl.")
                                     return
                                 skip = True
                
                if skip:
                    continue

                print(f"  Fetching detail page {i+1}/{num_results}...")
                self.process_detail_page(full_url)
                self.random_sleep()

            # Pagination
            # Look for active page in pager, then find the next one
            # Logic: Look for the link with title="Nächste Seite"
            next_link = soup.select_one('a[title="Nächste Seite"]')
            if next_link:
                next_url = urljoin(self.base_url, next_link.get('href'))
                print(f"Navigating to next page: {next_url}")
                resp = self.fetch_url(next_url)
                if resp.status_code != 200:
                    print(f"Failed to load next page: {resp.status_code}")
                    break
                soup = BeautifulSoup(resp.text, 'html.parser')
                page_num += 1
                self.random_sleep(1, 2)
            else:
                print("No next page found. Crawling finished.")
                break

    def process_detail_page(self, url):
        try:
            resp = self.fetch_url(url)
            if resp.status_code != 200:
                print(f"  Failed to load detail page: {resp.status_code}")
                return

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            data = {
                "crawled_at": time.time(),
                "url": url,
                "metadata": {},
                "content": ""
            }

            # Metadata Extraction
            # 1. From #doc-metadata (contains unified string like "BayObLG, Beschluss v. 21.01.2026 – 102 Sch 78/25 e")
            doc_metadata_div = soup.select_one('#doc-metadata')
            if doc_metadata_div:
                text = doc_metadata_div.get_text(strip=True)
                data["metadata"]["header_text"] = text
                
                # Split by comma to get court
                parts = text.split(',')
                if len(parts) > 0:
                     data["metadata"]["Gericht"] = parts[0].strip()
                
                # Try to extract Date and Az
                # Format: "... v. DD.MM.YYYY – Az..."
                if " v. " in text:
                    date_part = text.split(" v. ")[1]
                    # date_part might looks like "21.01.2026 – 102 Sch 78/25 e"
                    if "–" in date_part: # En-dash
                        date_str, az_str = date_part.split("–", 1)
                        data["metadata"]["Entscheidungsdatum"] = date_str.strip()
                        data["metadata"]["Aktenzeichen"] = az_str.strip()
                    elif "-" in date_part: # Hyphen (fallback)
                        date_str, az_str = date_part.split("-", 1)
                        data["metadata"]["Entscheidungsdatum"] = date_str.strip()
                        data["metadata"]["Aktenzeichen"] = az_str.strip()

            # 2. From .rsprbox (sidebar/top info box)
            # Contains fields like "Fundstelle:", "Titel:", "Schlagworte:"
            rsprbox = soup.select_one('.rsprbox')
            if rsprbox:
                current_key = None
                for child in rsprbox.children:
                    if child.name == 'div' and 'rsprboxueber' in child.get('class', []):
                        current_key = child.get_text(strip=True).replace(':', '')
                    elif child.name == 'div' and 'rsprboxzeile' in child.get('class', []) and current_key:
                        data["metadata"][current_key] = child.get_text(strip=True)
                    elif child.name == 'h1' and 'titelzeile' in child.get('class', []):
                         data["metadata"]["Titel"] = child.get_text(strip=True)

            # Content Extraction
            content_div = soup.select_one('.cont')
            if content_div:
                data["content"] = content_div.get_text(separator="\n", strip=True)
                data["html_content"] = str(content_div)
            else:
                # Fallback
                content_div = soup.select_one('#docbody')
                if content_div:
                    data["content"] = content_div.get_text(separator="\n", strip=True)
                    data["html_content"] = str(content_div)

            # Determine Filename
            az = data["metadata"].get("Aktenzeichen", "unknown").replace("/", "_").replace(" ", "_")
            if az == "unknown":
                 # Fallback to header text extraction if explicit field failed
                 if "header_text" in data["metadata"]:
                     pass # Already tried
                 
                 # Random fallback
                 az = f"decision_{int(time.time())}_{random.randint(1000,9999)}"

            filename = f"{az}.json"
            output_path = os.path.join(self.output_dir, filename)
            
            # Check overwrite (double check before saving, though we checked list item)
            if os.path.exists(output_path) and not self.overwrite:
                print(f"  Skipping existing file (found during save): {filename}")
                self.consecutive_skips += 1
                return

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"  Saved {filename}")
            self.crawled_count += 1
            self.consecutive_skips = 0

        except Exception as e:
            print(f"  Error extracting data from {url}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bayern Landesrecht Crawler")
    parser.add_argument("--limit", type=int, help="Limit number of decisions to crawl", default=None)
    parser.add_argument("--no-headless", action="store_true", help="Run with visible browser window (ignored, uses requests)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--stealth", action="store_true", help="Use browser-like User-Agent instead of crawler UA")
    parser.add_argument("--skip-threshold", type=int, default=50, help="Stop after N consecutive existing files (0=disable)")
    parser.add_argument("--output", type=str, default="data/bayern_raw", help="Output directory")

    args = parser.parse_args()

    crawler = BayernCrawler(output_dir=args.output, limit=args.limit, overwrite=args.overwrite, skip_threshold=args.skip_threshold, stealth=args.stealth)
    crawler.crawl()
