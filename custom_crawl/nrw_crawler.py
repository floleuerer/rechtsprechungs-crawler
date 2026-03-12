import os
import time
import json
import argparse
import random
from urllib.parse import urlparse
import traceback

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

class NRWCrawler:
    DEFAULT_USER_AGENT = "LegalCrawl/0.1 (legal research crawler)"
    STEALTH_USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    def __init__(self, output_dir="data/nrw_raw", headless=True, limit=None, overwrite=False, skip_threshold=50, stealth=False):
        self.output_dir = output_dir
        self.headless = headless
        self.limit = limit
        self.overwrite = overwrite
        self.skip_threshold = skip_threshold
        self.stealth = stealth
        self.base_url = "https://nrwesuche.justiz.nrw.de/index.php#form_anchor"
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
            
            # --- 1. Form Interaction ---
            print("Interacting with Search Form...")
            try:
                # Open advanced search
                adv_search_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#erweiterte_suche")))
                adv_search_btn.click()
                
                # Wait for date fields
                start_date_input = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#von")))
                end_date_input = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#bis")))
                
                # Clear and enter dates
                start_date_input.clear()
                start_date_input.send_keys("01.01.1900")
                
                end_date_input.clear()
                end_date_input.send_keys("01.01.2050")
                
                # Submit
                search_btn = self.driver.find_element(By.CSS_SELECTOR, "#absenden")
                search_btn.click()
                
            except TimeoutException:
                print("Error: Could not find or interact with search form.")
                return

            # --- 2. Process Results ---
            print("Waiting for results...")
            
            page_num = 1
            while True:
                print(f"--- Processing Result Page {page_num} ---")
                
                try:
                    # Wait for the results container
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.alleErgebnisse")))
                    # Find all links within the container
                    links = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.einErgebnis a")))
                except TimeoutException:
                    print("No results found or timeout waiting for result list.")
                    break

                # Filter valid links
                result_links = []
                for link in links:
                    href = link.get_attribute('href')
                    if href and 'nrwe.justiz.nrw.de' in href:
                         result_links.append(href)
                
                num_results = len(result_links)
                print(f"Found {num_results} results on this page.")

                if num_results == 0:
                    break
                
                for i, link_url in enumerate(result_links):
                    if self.limit and self.crawled_count >= self.limit:
                        print(f"Limit of {self.limit} reached.")
                        return

                    # Check if already processed (simple filename check)
                    # Extract ID from URL for filename
                    # URL format: .../4_O_461_96urteil19971021.html
                    try:
                        filename = link_url.split('/')[-1].replace('.html', '.json')
                        if not self.overwrite and os.path.exists(os.path.join(self.output_dir, filename)):
                            print(f"  Skipping existing: {filename}")
                            self.consecutive_skips += 1
                            if self.skip_threshold and self.consecutive_skips >= self.skip_threshold:
                                print(f"Reached {self.skip_threshold} consecutive existing files. Stopping incremental crawl.")
                                return
                            continue
                    except:
                        filename = None

                    print(f"  Accessing result {i+1}/{num_results}...")
                    
                    # Open in new tab
                    self.driver.execute_script("window.open(arguments[0], '_blank');", link_url)
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    
                    try:
                        # Wait for content
                        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        
                        self.extract_and_save(filename)
                        self.crawled_count += 1
                        self.consecutive_skips = 0
                        
                    except TimeoutException:
                        print("  Timeout waiting for detail page.")
                    except Exception as e:
                        print(f"  Error processing detail page: {e}")
                    finally:
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                        self.random_sleep(0.5, 1.0)

                # --- 3. Pagination ---
                print("Checking for next page...")
                try:
                    # Look for the input button with value '>'
                    next_button = self.driver.find_element(By.CSS_SELECTOR, "input.button[value='>']")
                    
                    # Store current results to wait for staleness
                    old_results = self.driver.find_element(By.CSS_SELECTOR, "div.alleErgebnisse")

                    print("Clicking Next button...")
                    next_button.click()
                    
                    # Wait for old results to disappear/refresh
                    wait.until(EC.staleness_of(old_results))
                    
                    page_num += 1
                    self.random_sleep(2, 4)

                except NoSuchElementException:
                    print("No 'Next' button found (or end of list).")
                    break
                except Exception as e:
                    print(f"Error navigating to next page: {e}")
                    break

        except Exception as e:
            print(f"Critical Error: {e}")
            traceback.print_exc()
        finally:
            if self.driver:
                self.driver.quit()

    def extract_and_save(self, filename_hint=None):
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            data = {
                "crawled_at": time.time(),
                "url": self.driver.current_url,
                "metadata": {},
                "content": ""
            }

            # NRW Content structure is often just text in body or specific divs
            # We'll grab the whole body text for now as it's the most reliable
            # Clean up script/style
            for tag in soup(['script', 'style', 'noscript', 'iframe']):
                tag.decompose()

            # Metadata extraction
            # Often at the top: "Gericht: ...", "Datum: ...", "Aktenzeichen: ..."
            # Look for these patterns in the text
            text_lines = list(soup.stripped_strings)
            data["content"] = "\n".join(text_lines)
            data["html_content"] = str(soup.body) if soup.body else str(soup)

            # Specific NRW Metadata Extraction using classes
            # Structure: <div class="feldbezeichnung">Key:</div><div class="feldinhalt">Value</div>
            field_labels = soup.find_all("div", class_="feldbezeichnung")
            for label_div in field_labels:
                key = label_div.get_text(strip=True).replace(':', '')
                
                # The value is usually in the next sibling div with class 'feldinhalt'
                # But sometimes there are newlines/spaces in between in source, so we find next sibling element
                value_div = label_div.find_next_sibling("div", class_="feldinhalt")
                if value_div:
                    val = value_div.get_text(strip=True)
                    if key and val:
                        data["metadata"][key] = val

            # Fallback/Additional Metadata Parsing from text lines if needed
            # (Keeping the loop for other potential formats, but reducing scope if structured data found)
            if not data["metadata"]:
                for line in text_lines[:50]: # Check first 50 lines for metadata
                    if ':' in line:
                        parts = line.split(':', 1)
                        key = parts[0].strip()
                        val = parts[1].strip()
                        
                        # Normalize key check
                        for meta_key in metadata_keys:
                            if meta_key.lower() in key.lower() and len(key) < 40:
                                data["metadata"][meta_key] = val
                                break

            # Filename
            if not filename_hint:
                filename_hint = f"doc_{int(time.time())}.json"
            
            output_path = os.path.join(self.output_dir, filename_hint)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"  Saved {filename_hint}")

        except Exception as e:
            print(f"  Error extracing data: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NRW Justiz Crawler")
    parser.add_argument("--limit", type=int, help="Limit number of decisions to crawl", default=None)
    parser.add_argument("--no-headless", action="store_true", help="Run with visible browser window")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--stealth", action="store_true", help="Use browser-like User-Agent instead of crawler UA")
    parser.add_argument("--skip-threshold", type=int, default=50, help="Stop after N consecutive existing files (0=disable)")
    parser.add_argument("--output", type=str, default="data/nrw_raw", help="Output directory")

    args = parser.parse_args()

    crawler = NRWCrawler(output_dir=args.output, headless=not args.no_headless, limit=args.limit, overwrite=args.overwrite, skip_threshold=args.skip_threshold, stealth=args.stealth)
    crawler.crawl()
