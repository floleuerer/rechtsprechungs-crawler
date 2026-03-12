import requests
from bs4 import BeautifulSoup
import os
import time
import json
import urllib.parse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

def is_retryable_error(exception):
    return (
        isinstance(exception, requests.exceptions.HTTPError) 
        and exception.response is not None 
        and exception.response.status_code == 429
    )

class BVerfGCrawler:
    START_URL = "https://www.bundesverfassungsgericht.de/SiteGlobals/Forms/Suche/Entscheidungssuche/Entscheidungssuche_Formular.html?nn=68038&callerId.HASH=f128d10b136549a0cdada320453a921d540714a791d7&cl2HtmlFragments_Verfahrensart.HASH=5078d5940d19c04618fdb585f400299de98c12c1054d&timerange.HASH=599de67b36b2cc457e8f817f7bbb88324cb3839d6019&eclidentifier_facet.HASH=578a38cbde372101383bd73bc9d83f8917714fcc1df6&entscheidungsdatum_facet.HASH=f88e8e9e4327d38d34384b51782aaa98eda5325032e0&callerId=148438&fundstelle_facet.HASH=94298f275f2d350cb32265e67fcd13e8a09e062c7a55&cl2TextFragments_Entscheidungstyp.HASH=cb5dde0fb5cfca6ebfcf3877f971d86f14cbb41452d3&autosuggest_facet.HASH=a2a7e6cc3b7476a9601beafa59b3f150f5d36ae6d042&aktenzeichen_facet.HASH=09c55609195d831cd983f5f4f8a9b3143220c6a407b3"

    DEFAULT_USER_AGENT = "LegalCrawl/0.1 (legal research crawler)"

    def __init__(self, output_dir="data/bverfg", limit=None, overwrite=False, skip_threshold=50, stealth=False):
        self.output_dir = output_dir
        self.limit = limit
        self.overwrite = overwrite
        self.skip_threshold = skip_threshold
        self.stealth = stealth
        self.base_url = "https://www.bundesverfassungsgericht.de"
        self.crawled_count = 0
        self.consecutive_skips = 0
        self.session = requests.Session()
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36" if self.stealth else self.DEFAULT_USER_AGENT
        self.session.headers.update({"User-Agent": ua})

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(is_retryable_error)
    )
    def fetch_url(self, url):
        response = self.session.get(url)
        response.raise_for_status()
        return response

    def crawl(self):
        current_url = self.START_URL
        page_count = 1
        
        while current_url:
            print(f"Crawling results page {page_count}: {current_url}")
            try:
                response = self.fetch_url(current_url)
            except requests.RequestException as e:
                print(f"Error fetching {current_url}: {e}")
                break

            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Process results on the current page
            self.process_results(soup)
            
            # Find next page
            next_button = soup.select_one('a.c-pagination__button--next')
            if next_button and 'href' in next_button.attrs:
                next_url = next_button['href']
                # Ensure absolute URL
                current_url = urllib.parse.urljoin(self.base_url, next_url)
                page_count += 1
                time.sleep(1) # Be polite
            else:
                print("No more pages found.")
                current_url = None

    def process_results(self, soup):
        results = soup.select('.l-search-wrapper__result-item')
        print(f"Found {len(results)} results on this page.")

        for result in results:
            if self.limit and self.crawled_count >= self.limit:
                print(f"Limit of {self.limit} reached.")
                return

            if self.skip_threshold and self.consecutive_skips >= self.skip_threshold:
                print(f"Reached {self.skip_threshold} consecutive existing files. Stopping incremental crawl.")
                return

            link_elem = result.select_one('a.c-teaser-search-result__link')
            if link_elem and 'href' in link_elem.attrs:
                detail_url = urllib.parse.urljoin(self.base_url, link_elem['href'])
                self.download_decision(detail_url)

    def download_decision(self, url):
        # Determine filename first to check existence
        path = urllib.parse.urlparse(url).path
        filename_base = os.path.basename(path)
        if not filename_base or filename_base == 'Entscheidung_Formular.html':
             # fallback if structure is weird
             filename_base = f"decision_{hash(url)}"
        
        output_path = os.path.join(self.output_dir, f"{filename_base}.json")

        if not self.overwrite and os.path.exists(output_path):
            print(f"  Skipping existing: {filename_base}.json")
            self.consecutive_skips += 1
            return

        print(f"  Downloading decision: {url}")
        try:
            response = self.fetch_url(url)
        except requests.RequestException as e:
            print(f"  Error fetching decision {url}: {e}")
            return
        
        time.sleep(0.5) # Be polite when actually downloading

        soup = BeautifulSoup(response.content, 'html.parser')
        
        data = {
            "url": url,
            "crawled_at": time.time(),
            "metadata": {},
            "content": ""
        }

        # Main content container
        main_content = soup.select_one('main') or soup.select_one('.c-article')
        
        if main_content:
            # Extract metadata if possible (this is heuristic based on observation)
            # Often h1 is title/date, h2 is Aktenzeichen
            h1 = main_content.select_one('h1')
            if h1:
                data["metadata"]["title"] = h1.get_text(strip=True)
            
            # Extract Aktenzeichen
            rubrum = main_content.select_one('.c-decision__rubrum')
            if rubrum:
                import re
                aktenzeichen_list = []
                # Look for patterns like "1 BvR 2368/24"
                # Sometimes they are in <p> tags, sometimes with hyphens: "- 1 BvR 2368/24 -"
                text = rubrum.get_text(separator="\n")
                # Pattern: digit(s) space Bv[Chars] space digit(s)/digit(s)
                matches = re.findall(r'(\d+\s+Bv[A-Za-z]+\s+\d+/\d+)', text)
                if matches:
                    # Deduplicate and sort
                    data["metadata"]["aktenzeichen"] = sorted(list(set(matches)))

            # Extract ECLI
            ecli_widget = main_content.select_one('.l-widget__content p') # ECLI is often in the first widget
            # Or look for text "European Case Law Identifier"
            for widget in main_content.select('.l-widget'):
                 header = widget.select_one('.l-widget__headline')
                 if header and 'European Case Law Identifier' in header.get_text():
                     content = widget.select_one('.l-widget__content p')
                     if content:
                         data["metadata"]["ecli"] = content.get_text(strip=True)

            # Simple text extraction
            data["content"] = main_content.get_text(separator="\n", strip=True)
            data["html_content"] = str(main_content)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.crawled_count += 1
        self.consecutive_skips = 0
        print(f"  Saved to {output_path}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bundesverfassungsgericht Crawler")
    parser.add_argument("--limit", type=int, help="Limit number of decisions to crawl", default=None)
    parser.add_argument("--no-headless", action="store_true", help="Run with visible browser window (ignored, uses requests)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--stealth", action="store_true", help="Use browser-like User-Agent instead of crawler UA")
    parser.add_argument("--skip-threshold", type=int, default=50, help="Stop after N consecutive existing files (0=disable)")
    parser.add_argument("--output", type=str, default="data/bverfg", help="Output directory")

    args = parser.parse_args()

    crawler = BVerfGCrawler(output_dir=args.output, limit=args.limit, overwrite=args.overwrite, skip_threshold=args.skip_threshold, stealth=args.stealth)
    crawler.crawl()
