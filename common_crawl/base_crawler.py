import os
import time
import json
import re
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

class BaseCommonCrawler:
    DEFAULT_USER_AGENT = "LegalCrawl/0.1 (legal research crawler)"
    STEALTH_USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]

    def __init__(self, output_dir, headers_selector, base_url, headless=True, limit=None, overwrite=False, skip_threshold=50, stealth=False, filter_id="categories_select_Rechtsprechung", filter_text="Rechtsprechung"):
        self.output_dir = output_dir
        self.headless = headless
        self.limit = limit
        self.overwrite = overwrite
        self.skip_threshold = skip_threshold
        self.stealth = stealth
        self.base_url = base_url
        self.driver = None
        self.crawled_count = 0
        self.consecutive_skips = 0
        self.headers_selector = headers_selector
        self.filter_id = filter_id
        self.filter_text = filter_text

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

    def expand_metadata(self):
        """
        New feature: Click "... mehr" buttons in metadata to reveal full content (e.g., Normen).
        """
        try:
            # Look for links that contain "mehr" text inside the document header
            # This is a generic approach; we can refine selectors if needed.
            # Based on user snippet: <span title="">...&nbsp;mehr</span> inside an <a>
            
            # We look for 'expand' links specifically in the header area
            header = self.driver.find_elements(By.CSS_SELECTOR, ".documentHeader, .docheader")
            if not header:
                return

            header_elem = header[0]
            
            # Find all links that might be "show more" buttons
            # Inspecting text or title or specific class if constant
            # User snippet had specific data-juris-gui="link" but text "... mehr" is reliably
            
            # Using xpath to find elements with "mehr" in text
            more_buttons = header_elem.find_elements(By.XPATH, ".//a[contains(., 'mehr')]")
            
            for btn in more_buttons:
                if btn.is_displayed():
                    try:
                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", btn)
                        self.random_sleep(0.2, 0.5)
                        self.driver.execute_script("arguments[0].click();", btn)
                        print("  Clicked '... mehr' button in metadata.")
                        self.random_sleep(0.5, 1.0) # Wait for expansion
                    except Exception as e:
                        print(f"  Failed to click 'mehr' button: {e}")

        except Exception as e:
            print(f"  Error in expand_metadata: {e}")

    def crawl(self):
        self.setup_driver()
        try:
            print(f"Opening {self.base_url}...")
            self.driver.get(self.base_url)
            
            # Wait for Sidebar and Click Filter (default "Rechtsprechung")
            print(f"Waiting for '{self.filter_text}' filter...")
            wait = WebDriverWait(self.driver, 15)
            
            try:
                # Try ID first (more robust)
                rechtsprechung_link = wait.until(EC.element_to_be_clickable((By.ID, self.filter_id)))
            except:
                # Fallback to text
                rechtsprechung_link = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, f"//div[contains(@class, 'category-select')]//a[contains(text(), '{self.filter_text}')]")
                ))
            
            rechtsprechung_link.click()
            print("Clicked 'Rechtsprechung'. Waiting for results...")
            self.random_sleep(2, 4) # Allow React to update the list

            # Wait for any loading spinner to disappear
            try:
                wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loading_msg")))
            except:
                pass

            page_num = 1
            while True:
                print(f"--- Processing Result Page {page_num} ---")
                
                # Wait for results to be present
                try:
                    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.result-list__entry-link")))
                except TimeoutException:
                    print("No results found or page load timed out.")
                    break

                # Get all result links on the current page
                result_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.result-list__entry-link")
                num_results = len(result_elements)
                print(f"Found {num_results} results on this page.")

                if num_results == 0:
                    break
                
                for i in range(num_results):
                    if self.limit and self.crawled_count >= self.limit:
                        print(f"Limit of {self.limit} reached.")
                        return

                    # Re-find elements to ensure freshness
                    try:
                        results = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.result-list__entry-link")))
                        if i >= len(results):
                            print(f"  Index {i} out of bounds (results changed?). Skipping.")
                            continue
                            
                        current_result = results[i]
                        
                        # Optimization: Check if already downloaded
                        try:
                            # Extract ID from the URL (href)
                            # href example: .../document/NJRE001231569
                            link_url = current_result.get_attribute('href')
                            if link_url:
                                parsed_url = urlparse(link_url)
                                path_parts = parsed_url.path.strip('/').split('/')
                                doc_id = None
                                
                                if "document" in path_parts:
                                    idx = path_parts.index("document")
                                    if idx + 1 < len(path_parts):
                                        doc_id = path_parts[idx + 1]
                                
                                # Fallback (maybe use last part if document not found? Or skip if not found?)
                                if not doc_id and path_parts:
                                    doc_id = path_parts[-1]

                                if doc_id:
                                    # Sanitize to match save logic
                                    doc_id_clean = "".join(c for c in doc_id if c.isalnum() or c in ('-', '_', '.'))
                                    
                                    filename_check = f"{doc_id_clean}.json"
                                    # Use absolute path for reliability
                                    abs_output_dir = os.path.abspath(self.output_dir)
                                    file_path_check = os.path.join(abs_output_dir, filename_check)
                                    
                                    # print(f"DEBUG Check: ID={doc_id_clean} -> Path={file_path_check} -> Exists={os.path.exists(file_path_check)}")
                                    
                                    if not self.overwrite and os.path.exists(file_path_check):
                                        print(f"  Skipping existing file: {filename_check}")
                                        self.consecutive_skips += 1
                                        if self.skip_threshold and self.consecutive_skips >= self.skip_threshold:
                                            print(f"Reached {self.skip_threshold} consecutive existing files. Stopping incremental crawl.")
                                            return
                                        continue
                        except Exception as e:
                            # print(f"DEBUG: Error in duplicate check: {e}")
                            pass
                        
                        # Scroll into view
                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", current_result)
                        self.random_sleep(0.5, 1)

                        print(f"  Accessing result {i+1}/{num_results}...")
                        
                        # Open in new tab to preserve pagination state
                        link_url = current_result.get_attribute('href')
                        self.driver.execute_script("window.open(arguments[0], '_blank');", link_url)
                        self.driver.switch_to.window(self.driver.window_handles[-1])
                        
                        # Wait for detail page content to actually load
                        try:
                            # 1. Wait for specific loading classes to disappear
                            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".docbody--loading, .docview__docbody--loading")))
                            
                            # 2. Wait for the content itself to be present
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".documentHeader, .docheader, .docframebs__content")))
                            
                            # 3. Double check: Ensure the article tag doesn't have the loading class (sometimes invisibility check is subtle)
                            # We can wait until an article with .docbody exists AND does NOT have .docbody--loading
                            # CSS selector for "article.docbody expecting no loading class" is hard in CSS (requires :not), 
                            # but we can rely on python check or better explicit wait.
                            wait.until(lambda d: d.find_elements(By.CSS_SELECTOR, "article.docbody:not(.docbody--loading):not(.docview__docbody--loading)")) or \
                                                 d.find_elements(By.CSS_SELECTOR, ".docframebs__content")

                        except TimeoutException:
                            print("  Timeout waiting for detail page load (loading spinner didn't disappear).")
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                            continue
                        
                        # NEW: Expand metadata if "mehr" buttons present
                        self.expand_metadata()

                        # Process Data
                        self.extract_and_save()
                        self.crawled_count += 1
                        self.consecutive_skips = 0
                        
                        # Close tab and go back to main window
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                        
                        # No need to wait for list reload as we never left the page
                        self.random_sleep(0.1, 0.3)
                        
                    except (StaleElementReferenceException, TimeoutException, Exception) as e:
                        print(f"  Error processing result {i+1}: {e}")
                        # Ensure we are on the main window if something went wrong
                        if len(self.driver.window_handles) > 1:
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                        continue

                # Pagination: Find "Next" button
                print("Checking for next page...")
                try:
                    # Capture a FRESH element from the current page to check for staleness later
                    current_results = self.driver.find_elements(By.CSS_SELECTOR, "a.result-list__entry-link")
                    if not current_results:
                        reference_element = None
                    else:
                        reference_element = current_results[0]
                    
                    self.random_sleep(0.5, 1)
                    # Look for the "Next" buttons. 
                    # Selector identified: a.pager__to-page--next
                    next_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a.pager__to-page--next")
                    
                    if not next_buttons:
                        print("No 'Next' button found. End of crawling.")
                        break

                    # Use the last button (bottom pagination)
                    next_button = next_buttons[-1]
                    
                    if "disabled" in next_button.get_attribute("class"):
                         print("Next button disabled. End of results.")
                         break
                    
                    # Scroll to bottom to ensure it's clickable
                    self.driver.execute_script("arguments[0].scrollIntoView();", next_button)
                    self.random_sleep(0.5, 1)
                    
                    print(f"Clicking Next button (found {len(next_buttons)} buttons)...")
                    # Use JS click for reliability
                    self.driver.execute_script("arguments[0].click();", next_button)
                    self.random_sleep(0.5, 1)
                    
                    # Wait for the OLD page content to disappear (become stale)
                    if reference_element:
                        print("Waiting for page update (staleness check)...")
                        try:
                            wait.until(EC.staleness_of(reference_element))
                        except TimeoutException:
                            print("  Timed out waiting for staleness. Proceeding hoping page updated...")
                    
                    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.result-list__entry-link")))
                    
                    page_num += 1
                    self.random_sleep(2, 4) 
                    
                except NoSuchElementException:
                    print("No 'Next' button found. End of crawling.")
                    break
                except Exception as e:
                    print(f"Error navigating to next page: {e}")
                    break

        finally:
            if self.driver:
                self.driver.quit()

    def extract_and_save(self):
        max_retries = 3
        for attempt in range(max_retries + 1):
            try:
                # We can use BeautifulSoup for parsing the static HTML of the current state
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Check for loading state content in soup
                loading_indicator = soup.select_one(".docbody--loading, .docview__docbody--loading, .docview__loading")
                
                # Also check if content div says "Dokument wird geladen"
                content_text_check = soup.get_text()
                if "Dokument wird geladen" in content_text_check:
                    loading_indicator = True

                if loading_indicator:
                    if attempt < max_retries:
                        print(f"  Warning: Detected loading state in HTML content. Retrying ({attempt+1}/{max_retries})...")
                        self.random_sleep(2, 3)
                        continue
                    else:
                        print("  Error: Meaningful content did not load after retries. Skipping file.")
                        # Return early to avoid saving bad data
                        return
                
                data = {
                    "crawled_at": time.time(),
                    "url": self.driver.current_url,
                    "metadata": {},
                    "content": ""
                }

                # Header / Metadata Extraction
                # Target selectors: .documentHeader, .docheader
                # Additional generic check for any table in header
                doc_header = soup.select_one(self.headers_selector)
                if doc_header:
                    # Extract Title
                    title_elem = doc_header.select_one('h1, .title')
                    if title_elem:
                        data["metadata"]["title"] = title_elem.get_text(strip=True)
                    
                    # Extract Table info
                    for row in doc_header.select('tr'):
                        th = row.select_one('th')
                        td = row.select_one('td')
                        if th and td:
                            key = th.get_text(strip=True).replace(':', '')
                            val = td.get_text(strip=True)
                            # Clean up "... weniger" artifacts from expanded metadata
                            # Use regex to handle non-breaking spaces etc.
                            val = re.sub(r'\.\.\.\s*weniger$', '', val).replace("... mehr", "").strip()
                            data["metadata"][key] = val

                # Fallback Metadata extraction from text if table missing
                # e.g. "Gericht: VGH Baden-Württemberg" in text
                if not data["metadata"]:
                    header_text = doc_header.get_text() if doc_header else ""
                    lines = header_text.split('\n')
                    for line in lines:
                        if ':' in line:
                            parts = line.split(':', 1)
                            if len(parts) == 2:
                                 key = parts[0].strip()
                                 val = parts[1].strip()
                                 data["metadata"][key] = val

                # Main Content Extraction
                # Prioritize the article body which contains the actual text
                content_div = soup.select_one('article.docbody, .docbody, .jur-ebener-view, #docbody, .docviewmain, .docLayoutText')
                
                # If nothing found, try broader containers (fallback)
                if not content_div:
                    content_div = soup.select_one('.docframebs__content')

                if content_div:
                    # Cleanup: Remove Header, Navigation, Tabs, etc. from content
                    # Add .doctocnav, .doctabs to the blacklist
                    for useless in content_div.select('.documentHeader, .docheader, .docreiter, .docStructure, .marginal, .docNavigation, .doctocnav, .doctabs'):
                        useless.decompose()
                    
                    data["content"] = content_div.get_text(separator="\n", strip=True)
                    data["html_content"] = str(content_div)
                else:
                    print("  Warning: No content div found.")
                
                # Create a filename
                # Use ID from URL: .../document/NJRE001231569/...
                try:
                    parsed_url = urlparse(data["url"])
                    path_parts = parsed_url.path.strip('/').split('/')
                    doc_id = None
                    
                    if "document" in path_parts:
                        idx = path_parts.index("document")
                        if idx + 1 < len(path_parts):
                            doc_id = path_parts[idx + 1]
                    
                    # Fallback if "document" not found or structure weird
                    if not doc_id:
                        if path_parts:
                            doc_id = path_parts[-1]
                        else:
                            doc_id = f"decision_{int(time.time())}"
                            
                except Exception:
                    doc_id = f"decision_{int(time.time())}"
                
                # Sanitize just in case
                doc_id = "".join(c for c in doc_id if c.isalnum() or c in ('-', '_', '.'))
                
                filename = f"{doc_id}.json"
                output_path = os.path.join(self.output_dir, filename)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                print(f"  Saved {filename}")
                break

            except Exception as e:
                print(f"  Error extracting data: {e}")
                if attempt < max_retries:
                     print("Retrying...")
                     self.random_sleep(1, 2)
                else:
                     break
