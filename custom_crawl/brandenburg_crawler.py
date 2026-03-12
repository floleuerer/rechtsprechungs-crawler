import os
import time
import json
import argparse
import random
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

class BrandenburgCrawler:
    DEFAULT_USER_AGENT = "LegalCrawl/0.1 (legal research crawler)"
    STEALTH_USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]

    def __init__(self, output_dir="data/brandenburg_raw", headless=True, limit=None, overwrite=False, skip_threshold=50, stealth=False):
        self.output_dir = output_dir
        self.headless = headless
        self.limit = limit
        self.overwrite = overwrite
        self.skip_threshold = skip_threshold
        self.stealth = stealth
        # Pre-configured URL with date range 1900-2040
        self.base_url = "https://gerichtsentscheidungen.brandenburg.de/suche?input_title_abr=&input_fulltext=&input_aktenzeichen=&input_ecli=&input_date_promulgation_from=1900-01-01&input_date_promulgation_to=2040-01-01&select_source=0&page=1#"
        self.driver = None
        self.crawled_count = 0
        self.consecutive_skips = 0

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def setup_driver(self):
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        if self.stealth:
            options.add_argument(f"user-agent={random.choice(self.STEALTH_USER_AGENTS)}")
        else:
            options.add_argument(f"user-agent={self.DEFAULT_USER_AGENT}")

        self.driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=options
        )

    def random_sleep(self, min_sec=1.5, max_sec=3.0):
        time.sleep(random.uniform(min_sec, max_sec))

    def crawl(self):
        self.setup_driver()
        try:
            print(f"Opening {self.base_url}...")
            self.driver.get(self.base_url)
            
            wait = WebDriverWait(self.driver, 15)
            
            # Wait for results to load
            print("Waiting for results table...")
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table#resultlist")))
            except TimeoutException:
                print("Timeout waiting for result list.")
                return

            page_num = 1
            while True:
                print(f"--- Processing Result Page {page_num} ---")
                
                # Re-find the table rows to ensure fresh elements
                try:
                    rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table#resultlist tbody tr")))
                except TimeoutException:
                    print("No results found or page load timed out.")
                    break

                num_results = len(rows)
                print(f"Found {num_results} results on this page.")

                if num_results == 0:
                    break
                
                for i in range(num_results):
                    if self.limit and self.crawled_count >= self.limit:
                        print(f"Limit of {self.limit} reached.")
                        return

                    # Re-acquire rows to avoid StaleElementReferenceException
                    try:
                        rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table#resultlist tbody tr")))
                        if i >= len(rows):
                             print(f"  Index {i} out of bounds. Skipping.")
                             continue
                        
                        current_row = rows[i]
                        
                        # Find the link in this row (Bezeichnung column usually)
                        link_element = current_row.find_element(By.CSS_SELECTOR, "td a")
                        link_url = link_element.get_attribute('href')
                        
                        # Optimization: Check if already downloaded using ID from URL
                        # Brandenburg URL structure: /gerichtsentscheidung/[ID]
                        try:
                            path_parts = urlparse(link_url).path.split('/')
                            # Expecting ['', 'gerichtsentscheidung', 'ID']
                            if len(path_parts) >= 3 and path_parts[-2] == 'gerichtsentscheidung':
                                doc_id = path_parts[-1]
                                filename_check = f"{doc_id}.json"
                                file_path_check = os.path.join(self.output_dir, filename_check)
                                
                                if not self.overwrite and os.path.exists(file_path_check):
                                    print(f"  Skipping existing file: {filename_check}")
                                    self.consecutive_skips += 1
                                    if self.skip_threshold and self.consecutive_skips >= self.skip_threshold:
                                        print(f"Reached {self.skip_threshold} consecutive existing files. Stopping incremental crawl.")
                                        return
                                    continue
                            else:
                                doc_id = None
                        except Exception as e:
                            print(f"Error parsing link URL for ID: {e}")
                            doc_id = None
                        
                        print(f"  Accessing result {i+1}/{num_results}...")
                        
                        # Open in new tab
                        self.driver.execute_script("window.open(arguments[0], '_blank');", link_url)
                        self.driver.switch_to.window(self.driver.window_handles[-1])
                        
                        # Wait for detail content
                        try:
                            # Wait for the main content container
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.bb-border")))
                            # Also wait for a header to ensure it's not empty
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1, h2, h3")))
                        except TimeoutException:
                            print("  Timeout waiting for detail page load.")
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                            continue
                        
                        # Process Data
                        self.extract_and_save(doc_id)
                        self.crawled_count += 1
                        self.consecutive_skips = 0
                        
                        # Close tab
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                        
                        self.random_sleep(0.5, 1.0)
                        
                    except (StaleElementReferenceException, TimeoutException, NoSuchElementException, Exception) as e:
                        print(f"  Error processing result {i+1}: {e}")
                        if len(self.driver.window_handles) > 1:
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                        continue

                # Pagination
                print("Checking for next page...")
                try:
                    # Capture current state to check for staleness
                    # We can use the table itself
                    try:
                        old_table = self.driver.find_element(By.ID, "resultlist")
                    except NoSuchElementException:
                        old_table = None # Should not happen if we found rows
                    
                    next_button = self.driver.find_element(By.CSS_SELECTOR, "a[aria-label='Weiter']")
                    
                    # Check if disabled (often aria-disabled="true" or class class="disabled")
                    # Inspect showed: <li class="disabled"><a ...> but we selected the A tag directly.
                    # Parent LI might be disabled.
                    parent_li = next_button.find_element(By.XPATH, "..")
                    if "disabled" in parent_li.get_attribute("class"):
                        print("Next button disabled (parent class). End of results.")
                        break

                    print("Clicking Next button...")
                    self.driver.execute_script("arguments[0].scrollIntoView();", next_button)
                    self.driver.execute_script("arguments[0].click();", next_button)
                    
                    # Wait for staleness of the old table
                    if old_table:
                        try:
                            wait.until(EC.staleness_of(old_table))
                        except TimeoutException:
                            print("  Timed out waiting for table update. Proceeding...")

                    page_num += 1
                    self.random_sleep(2, 4)

                except NoSuchElementException:
                    print("No 'Next' button found (or end of list).")
                    break
                except Exception as e:
                    print(f"Error navigating to next page: {e}")
                    break

        finally:
            if self.driver:
                self.driver.quit()

    def extract_and_save(self, doc_id=None):
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            data = {
                "crawled_at": time.time(),
                "url": self.driver.current_url,
                "metadata": {},
                "content": ""
            }

            # Metadata is largely in the top area, sometimes in a 'Metadaten' section if expanded, 
            # or just the header info.
            # Based on inspection: "div.bb-border div.columns" contains content.
            
            # Title
            title_elem = soup.select_one('h1')
            if title_elem:
                data["metadata"]["title"] = title_elem.get_text(strip=True)

            # Extract fields from the visible text logic if needed, 
            # or look for specific metadata tables if they exist.
            # Brandenburg often puts Aktenzeichen in the title or a table.
            # Let's try to find key metadata.
            
            # Common structure: 
            # <h3>Metadaten</h3> ...
            # We can grab all text from the main container
            
            # Prefer #gerichtsentscheidung-detail to avoid nav/header noise
            content_div = soup.select_one('#gerichtsentscheidung-detail')
            if not content_div:
                content_div = soup.select_one('div.bb-border')
            
            if content_div:
                # Remove script/style
                for script in content_div(['script', 'style']):
                    script.decompose()
                
                data["content"] = content_div.get_text(separator="\n", strip=True)
                data["html_content"] = str(content_div)
                
                # Attempt to parse specific metadata from text if structure allows
                # E.g. "Aktenzeichen: ..."
                # Better approach: Parse the metadata table directly
                meta_table = soup.select_one('table.bb-table-stripes, #metadata table')
                if meta_table:
                    # The table often has multiple th/td pairs per row
                    # <tr><th>Key1</th><td>Val1</td><th>Key2</th><td>Val2</td></tr>
                    rows = meta_table.find_all('tr')
                    for row in rows:
                        cols = row.find_all(['th', 'td'])
                        # Iterate in pairs
                        for i in range(0, len(cols) - 1, 2):
                            key_elem = cols[i]
                            val_elem = cols[i+1]
                            if key_elem.name == 'th' and val_elem.name == 'td':
                                key = key_elem.get_text(strip=True).replace(':', '')
                                val = val_elem.get_text(strip=True)
                                if key and val:
                                    data["metadata"][key] = val
                
                # Fallback to text parsing if table didn't yield much
                if not data["metadata"]:
                    lines = data["content"].split('\n')
                    for line in lines:
                        if ':' in line:
                            parts = line.split(':', 1)
                            if len(parts) == 2:
                                key = parts[0].strip()
                                val = parts[1].strip()
                                # simple heuristic to keep likely metadata
                                if len(key) < 30 and len(val) < 200: 
                                    data["metadata"][key] = val

            # Filename generation
            # Use doc_id if provided (from URL), otherwise fallback
            if doc_id:
                filename = f"{doc_id}.json"
            else:
                # Fallback to URL ID extraction if not passed
                url_path = urlparse(self.driver.current_url).path
                if "/gerichtsentscheidung/" in url_path:
                    filename = f"{url_path.split('/')[-1]}.json"
                else:
                    # Last resort: timestamp
                    filename = f"decision_{int(time.time())}.json"

            output_path = os.path.join(self.output_dir, filename)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"  Saved {filename}")

        except Exception as e:
            print(f"  Error extracting data: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Brandenburg Landesrecht Crawler")
    parser.add_argument("--limit", type=int, help="Limit number of decisions to crawl", default=None)
    parser.add_argument("--no-headless", action="store_true", help="Run with visible browser window")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--stealth", action="store_true", help="Use browser-like User-Agent instead of crawler UA")
    parser.add_argument("--skip-threshold", type=int, default=50, help="Stop after N consecutive existing files (0=disable)")
    parser.add_argument("--output", type=str, default="data/brandenburg_raw", help="Output directory")

    args = parser.parse_args()

    crawler = BrandenburgCrawler(output_dir=args.output, headless=not args.no_headless, limit=args.limit, overwrite=args.overwrite, skip_threshold=args.skip_threshold, stealth=args.stealth)
    crawler.crawl()
