import argparse #imports = tools for the program
import csv
import os
import random
import re
import signal
import sqlite3
import sys
import time
from typing import Iterable, List, Optional, Sequence, Tuple

from selenium import webdriver #selenium webdriver controls a Chrome browser programmatically
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Configuration

BASE_URL = "https://tigernet.princeton.edu/people"  # Base URL (without filters)

# Filter parameters - add your filters here (will be appended to BASE_URL)
# Example for Class of 2005-1975:
FILTERS = "_f881447f_Primary_Class_Year=%5B%222005%22%2C%222004%22%2C%222003%22%2C%222002%22%2C%222001%22%2C%222000%22%2C%221999%22%2C%221998%22%2C%221997%22%2C%221996%22%2C%221995%22%2C%221994%22%2C%221993%22%2C%221992%22%2C%221991%22%2C%221990%22%2C%221989%22%2C%221988%22%2C%221987%22%2C%221986%22%2C%221985%22%2C%221984%22%2C%221983%22%2C%221982%22%2C%221981%22%2C%221980%22%2C%221979%22%2C%221978%22%2C%221977%22%2C%221976%22%2C%221975%22%5D"
# Set to empty string "" to scrape all alumni (no filter)
# FILTERS = ""

START_PAGE = 1                      # people?page=<n>; page 1 works without param
END_PAGE = None                     # set to an int to cap pages
HEADLESS = False                    # set True after you verify login works
PAGE_TIMEOUT = 18
DELAY_MIN = 2.2
DELAY_MAX = 5.2
MAX_USERS = 130_423


def build_url(page: int = 1) -> str:
    """Build the full URL with filters and pagination."""
    if FILTERS:
        if page > 1:
            return f"{BASE_URL}?{FILTERS}&page={page}"
        else:
            return f"{BASE_URL}?{FILTERS}"
    else:
        if page > 1:
            return f"{BASE_URL}?page={page}"
        else:
            return BASE_URL

# Regular Expressions (reg exs)
EMAIL_PATTERNS = [
    re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", re.IGNORECASE),
    re.compile(r"[a-zA-Z0-9._%+-]+[\s@]+[a-zA-Z0-9.-]+[\s.]+[a-zA-Z]{2,}", re.IGNORECASE),
    re.compile(r"[a-zA-Z0-9._%+-]+(?:\s*(?:at|@|&#64;)\s*)[a-zA-Z0-9.-]+(?:\s*(?:dot|\.|\[dot\])\s*)[a-zA-Z]{2,}", re.IGNORECASE),
]

# Pattern to extract LinkedIn profile URLs
LINKEDIN_PATTERN = re.compile(r'https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]+/?', re.IGNORECASE)

# Pattern to extract class year (4-digit year, typically 1900s-2000s)
CLASS_YEAR_PATTERN = re.compile(r'\b(19\d{2}|20\d{2})\b')

# Global state for graceful shutdown
_writer_instance = None
_interrupted = False


class SQLiteWriter:
    """SQLite database writer with auto-commit and resume capability."""
    
    def __init__(self, db_path: str = "alumni.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.row_count = 0
        self._create_table()
        # Get existing count for display
        self.cursor.execute("SELECT COUNT(*) FROM alumni")
        self.existing_count = self.cursor.fetchone()[0]
        if self.existing_count > 0:
            print(f"📂 Resuming: Found {self.existing_count} existing records in database")
    
    def _create_table(self) -> None:
        """Create the alumni table if it doesn't exist."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS alumni (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_url TEXT UNIQUE,
                name TEXT,
                email TEXT,
                linkedin TEXT,
                city TEXT,
                state TEXT,
                industry TEXT,
                work_title TEXT,
                firm_name TEXT,
                class_year TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Create index on profile_url for fast duplicate checking
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_profile_url ON alumni(profile_url)
        """)
        self.conn.commit()
    
    def is_scraped(self, profile_url: str) -> bool:
        """Check if a profile URL has already been scraped."""
        self.cursor.execute("SELECT 1 FROM alumni WHERE profile_url = ?", (profile_url,))
        return self.cursor.fetchone() is not None
    
    def write_row(self, profile_url: str, name: str, email: str, linkedin: str, city: str, state: str, 
                  industry: str, work_title: str, firm_name: str, class_year: str) -> bool:
        """Write a single row to the database. Returns True if inserted, False if duplicate."""
        try:
            self.cursor.execute("""
                INSERT INTO alumni (profile_url, name, email, linkedin, city, state, industry, work_title, firm_name, class_year)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (profile_url, name, email, linkedin, city, state, industry, work_title, firm_name, class_year))
            self.conn.commit()
            self.row_count += 1
            return True
        except sqlite3.IntegrityError:
            # Duplicate profile_url
            return False
    
    def get_total_count(self) -> int:
        """Get total number of records in database."""
        self.cursor.execute("SELECT COUNT(*) FROM alumni")
        return self.cursor.fetchone()[0]
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.commit()
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def export_to_csv(db_path: str = "alumni.db", csv_path: str = "alumni_export.csv") -> None:
    """Export the SQLite database to a CSV file."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name, email, linkedin, city, state, industry, work_title, firm_name, class_year
        FROM alumni
        ORDER BY class_year, name
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Email", "LinkedIn", "City", "State", "Industry", "Work Title", "Firm Name", "Class Year"])
        writer.writerows(rows)
    
    print(f"✓ Exported {len(rows)} records to {csv_path}")


def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) and SIGTERM gracefully."""
    global _interrupted, _writer_instance
    _interrupted = True
    print("\n\n⚠️  Interrupt received. Saving progress...")
    if _writer_instance:
        try:
            # Get count BEFORE closing
            total = _writer_instance.row_count + _writer_instance.existing_count
            db_path = _writer_instance.db_path
            _writer_instance.close()
            _writer_instance = None  # Clear reference to prevent double-close
            print(f"💾 Database saved: {total} total records in {db_path}")
        except Exception:
            pass
    print("Progress saved. You can resume anytime by running the script again.")
    sys.exit(0)

# extract emails from html blocks
def extract_emails_from_blocks(blocks: Sequence) -> List[str]:
    """Grab mailto anchors inside the email blocks, preserving order (primary first)."""
    seen = set()
    ordered = []
    for block in blocks:
        anchors = block.find_elements(By.CSS_SELECTOR, "a[href^='mailto:']")
        for anchor in anchors:
            href = anchor.get_attribute("href") or ""
            email = href.replace("mailto:", "").split("?", 1)[0].strip().lower()
            if email and email not in seen:
                seen.add(email)
                ordered.append(email)
    return ordered

# faster method with regex
def extract_emails_by_regex(text: str) -> List[str]:
    """Extract emails using pre-compiled regex patterns."""
    found = []
    for pattern in EMAIL_PATTERNS:
        for match in pattern.findall(text):
            cleaned = (
                match.lower()
                .replace(" at ", "@")
                .replace("[at]", "@")
                .replace(" dot ", ".")
                .replace("[dot]", ".")
            )
            domain_part = cleaned.split("@", 1)[1] if "@" in cleaned else ""
            if "@" in cleaned and "." in domain_part and cleaned not in found:
                found.append(cleaned)
    return found

# extract LinkedIn URL from page source
def extract_linkedin(page_source: str) -> str:
    """Extract LinkedIn profile URL from page source."""
    match = LINKEDIN_PATTERN.search(page_source)
    if match:
        return match.group(0).rstrip('/')
    return ""


# extract class year from structured element or text
def extract_class_year_from_text(text: str) -> str:
    """Extract a 4-digit class year from text."""
    match = CLASS_YEAR_PATTERN.search(text)
    if match:
        return match.group(1)
    return ""


def parse_location(location: str) -> Tuple[str, str]:
    """Parse location string into (city, state).
    
    Examples:
        "Vienna, VA, United States" -> ("Vienna", "VA")
        "New York, NY" -> ("New York", "NY")
        "San Francisco, California, USA" -> ("San Francisco", "California")
    """
    if not location:
        return "", ""
    
    parts = [p.strip() for p in location.split(",")]
    
    if len(parts) >= 2:
        city = parts[0]
        state = parts[1]
        return city, state
    elif len(parts) == 1:
        return parts[0], ""
    return "", ""

# web driver setup template
def build_driver() -> webdriver.Chrome:
    """Build an optimized Chrome driver with performance settings."""
    options = ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-notifications")
    options.add_argument("--start-maximized")
    
    # Performance optimizations
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    
    # Reduce logging overhead
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    # Reuse the existing Chrome/Edge profile if you need to stay logged in:
    # options.add_argument("--user-data-dir=/path/to/your/profile")
    return webdriver.Chrome(options=options, service=Service())

# find /user/hyperlinks on main /people page
def collect_profile_links(driver: webdriver.Chrome) -> List[str]:
    """Collect profile links from the listing page."""
    # Wait for profile cards to load - look for ANY link to /users/
    wait = WebDriverWait(driver, PAGE_TIMEOUT)
    wait.until(
        EC.presence_of_all_elements_located(
            (By.XPATH, "//a[contains(@href, '/users/')]")
        )
    )
    
    # Scroll to load all profiles on the page
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(5):  # Scroll multiple times to load all content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    # Scroll back to top
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.3)
    
    # Collect ALL links that point to /users/ profiles
    # Use a list to preserve order (alphabetical as shown on page)
    seen = set()
    ordered_links = []
    
    # Find all anchor tags with /users/ in href
    anchors = driver.find_elements(By.XPATH, "//a[contains(@href, '/users/')]")
    for anchor in anchors:
        try:
            href = anchor.get_attribute("href")
            if href and "/users/" in href:
                # Clean the URL (remove query params)
                clean_href = href.split("?", 1)[0]
                # Skip if already seen
                if clean_href not in seen:
                    seen.add(clean_href)
                    ordered_links.append(clean_href)
        except:
            continue
    
    return ordered_links


def scrape_profile(driver: webdriver.Chrome, url: str, include_all: bool) -> dict:
    """Scrape a profile using direct navigation (much faster than opening new windows).
    
    Returns: dict with keys: name, email, linkedin, city, state, industry, work_title, firm_name, class_year
    """
    # Initialize all fields
    result = {
        "name": "(unknown)",
        "email": "",
        "linkedin": "",
        "city": "",
        "state": "",
        "industry": "",
        "work_title": "",
        "firm_name": "",
        "class_year": ""
    }
    page_source = None  # Cache for regex fallback if needed

    try:
        # Direct navigation is much faster than opening/closing windows
        driver.get(url)
        
        try:
            WebDriverWait(driver, PAGE_TIMEOUT).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "[data-testid='display-attribute-email'], h1, [data-testid='profile-name']")
                )
            )
        except TimeoutException:
            print(f"  Timeout waiting for profile {url}")
            return result

        # Optimized name extraction: try most specific selectors first
        name_candidates = []
        name_selectors = [
            "h1[data-testid='profile-name']",
            "[data-testid='profile-name']",
            "[data-testid='member-name']",
            "h3.sc-braxZu.DwTMa",
            "header h1",
            "main h1",
            "h1",
        ]
        
        for selector in name_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elems:
                    text = (elem.text or "").strip()
                    if text:
                        lowered = text.lower()
                        if lowered not in ("princeton information", "contact", "experience", "education") and text not in name_candidates:
                            name_candidates.append(text)
                            break  # Found one, no need to check more elements for this selector
                if name_candidates:
                    break  # Found name, skip remaining selectors
            except WebDriverException:
                continue

        # Fallback: look for full name attribute if still not found
        if not name_candidates:
            try:
                full_name_elem = driver.find_element(
                    By.XPATH,
                    "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'full name')]/following::*[@data-testid='display-attribute-simple-string'][1]"
                )
                full_name_text = (full_name_elem.text or "").strip()
                if full_name_text:
                    lowered = full_name_text.lower()
                    if lowered not in ("princeton information", "contact"):
                        name_candidates.append(full_name_text)
            except WebDriverException:
                pass

        if name_candidates:
            result["name"] = name_candidates[0]

        # Extract emails from structured blocks (fastest method)
        email_blocks = driver.find_elements(By.CSS_SELECTOR, "[data-testid='display-attribute-email']")
        emails = extract_emails_from_blocks(email_blocks)

        if not include_all and emails:
            emails = emails[:1]

        # Fallback to regex only if no emails found (cache page_source to avoid multiple fetches)
        if not emails:
            if page_source is None:
                page_source = driver.page_source
            emails = extract_emails_by_regex(page_source)
            if not include_all and emails:
                emails = emails[:1]
        
        # Store primary email
        if emails:
            result["email"] = emails[0]

        # Extract Class Year - look for "Primary Class/Degree Year:" label
        try:
            # Method 1: Find the label and get the value (e.g., "1997" in div.bgAIqA)
            class_year_elems = driver.find_elements(
                By.XPATH,
                "//div[contains(text(), 'Primary Class/Degree Year')]/following-sibling::div//div[contains(@class, 'bgAIqA')]"
            )
            if class_year_elems:
                result["class_year"] = (class_year_elems[0].text or "").strip()
            
            # Method 2: Fallback - look for link with Primary_Class_Year in href
            if not result["class_year"]:
                class_year_links = driver.find_elements(
                    By.XPATH,
                    "//a[contains(@href, 'Primary_Class_Year')]//div[contains(@class, 'bgAIqA')]"
                )
                if class_year_links:
                    result["class_year"] = (class_year_links[0].text or "").strip()
            
            # Method 3: Fallback - extract from "Class of YYYY" in Cluster text
            if not result["class_year"]:
                cluster_elems = driver.find_elements(
                    By.XPATH,
                    "//div[contains(text(), 'Cluster')]/following-sibling::div"
                )
                for elem in cluster_elems:
                    text = elem.text or ""
                    import re
                    match = re.search(r'Class of (\d{4})', text)
                    if match:
                        result["class_year"] = match.group(1)
                        break
            
            # Method 4: Fallback - extract 'YY from name suffix (e.g., "Jennifer M. Abbondanza '97")
            if not result["class_year"] and result["name"]:
                import re
                match = re.search(r"'(\d{2})\b", result["name"])
                if match:
                    year_short = match.group(1)
                    # Convert 2-digit to 4-digit year (assume 1900s for 50-99, 2000s for 00-49)
                    year_int = int(year_short)
                    result["class_year"] = str(1900 + year_int) if year_int >= 50 else str(2000 + year_int)
        except WebDriverException:
            pass

        # Extract Location - look for "Location" label in header area
        location = ""
        try:
            # Method 1: Find Location label and get the sibling value (header section)
            location_elems = driver.find_elements(
                By.XPATH,
                "//div[contains(@class, 'bUABUj') and text()='Location']/following-sibling::div[contains(@class, 'bHjQkj')]"
            )
            if location_elems:
                location = (location_elems[0].text or "").strip()
            
            # Method 2: Fallback - look in contact section
            if not location:
                location_elems = driver.find_elements(
                    By.XPATH,
                    "//div[contains(@class, 'dZkXOh') and text()='Location']/following-sibling::div//div[@data-testid='display-attribute-map']//preceding-sibling::*"
                )
                if location_elems:
                    location = (location_elems[0].text or "").strip()
        except WebDriverException:
            pass
        
        # Parse location into city and state
        if location:
            result["city"], result["state"] = parse_location(location)

        # Extract LinkedIn URL from page source
        if page_source is None:
            page_source = driver.page_source
        result["linkedin"] = extract_linkedin(page_source)
        
        # Also try to find LinkedIn via anchor elements directly
        if not result["linkedin"]:
            try:
                linkedin_links = driver.find_elements(
                    By.XPATH,
                    "//a[contains(@href, 'linkedin.com/in/')]"
                )
                if linkedin_links:
                    result["linkedin"] = (linkedin_links[0].get_attribute("href") or "").strip()
            except WebDriverException:
                pass

        # Extract Work Title and Firm Name from Experience section (most recent/first entry)
        try:
            # Look for the experience section - job titles have class 'jcqcYi'
            # Structure: <div class="jcqcYi">Software Engineer</div> <div class="eQPLYZ">at</div> <div class="jcqcYi">Meta Platforms Inc.</div>
            job_title_elems = driver.find_elements(
                By.XPATH,
                "//div[contains(@class, 'jcqcYi')]"
            )
            if len(job_title_elems) >= 2:
                # First jcqcYi is the job title, second is the company name
                result["work_title"] = (job_title_elems[0].text or "").strip()
                result["firm_name"] = (job_title_elems[1].text or "").strip()
            elif len(job_title_elems) == 1:
                result["work_title"] = (job_title_elems[0].text or "").strip()
        except WebDriverException:
            pass

        # Extract Industry (Field/Specialty) from Experience section
        try:
            industry_elems = driver.find_elements(
                By.XPATH,
                "//div[contains(text(), 'Field/Specialty')]/following-sibling::div//div[contains(@class, 'chVwgM')]"
            )
            if industry_elems:
                result["industry"] = (industry_elems[0].text or "").strip()
        except WebDriverException:
            pass

    except WebDriverException as exc:
        print(f"  Error scraping profile {url}: {exc}")
    
    # Delay between profiles to avoid rate limiting
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
    return result


def iter_pages() -> Iterable[int]:
    if END_PAGE is None:
        page = START_PAGE
        while True:
            yield page
            page += 1
    else:
        for page in range(START_PAGE, END_PAGE + 1):
            yield page


def click_next_page(driver: webdriver.Chrome) -> bool:
    """Click the 'Next' pagination button. Returns True if successful, False if no more pages."""
    try:
        # Look for Next button/arrow in pagination
        next_buttons = driver.find_elements(
            By.XPATH,
            "//button[contains(@aria-label, 'Next') or contains(@aria-label, 'next')]"
            " | //a[contains(@aria-label, 'Next') or contains(@aria-label, 'next')]"
            " | //button[contains(text(), 'Next')]"
            " | //*[contains(@class, 'pagination')]//button[last()]"
            " | //*[contains(@class, 'pagination')]//a[last()]"
            " | //nav//button[contains(@class, 'next')]"
            " | //button[contains(@class, 'next')]"
            " | //*[@data-testid='pagination-next']"
        )
        
        for btn in next_buttons:
            # Check if button is enabled/clickable
            if btn.is_displayed() and btn.is_enabled():
                disabled = btn.get_attribute("disabled")
                aria_disabled = btn.get_attribute("aria-disabled")
                if disabled != "true" and aria_disabled != "true":
                    driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                    time.sleep(0.3)
                    btn.click()
                    time.sleep(2)  # Wait for content to load
                    return True
        
        # Try clicking by finding pagination and clicking the arrow/next element
        pagination = driver.find_elements(By.XPATH, "//*[contains(@class, 'pagination') or contains(@class, 'Pagination')]")
        if pagination:
            # Try to find a right arrow or ">" symbol
            arrows = pagination[0].find_elements(By.XPATH, ".//*[contains(text(), '›') or contains(text(), '>') or contains(text(), '→')]")
            if arrows and arrows[-1].is_displayed():
                arrows[-1].click()
                time.sleep(2)
                return True
        
        return False
    except WebDriverException:
        return False


def scrape_directory(db_path: str, include_all_emails: bool, target_emails: int) -> None:
    """Scrape directory and save to SQLite database with resume capability."""
    global _writer_instance, _interrupted
    
    collected_emails = 0
    driver = build_driver()
    
    # Create SQLite writer (auto-resumes if database exists)
    writer = SQLiteWriter(db_path)
    _writer_instance = writer  # Store reference for signal handler
    
    try:
        # Navigate to filtered URL
        start_url = build_url(page=1)
        driver.get(start_url)
        print(f"Navigating to: {start_url}")
        input("Log in manually (if needed), then press Enter to start scraping...")

        page = 1
        while not _interrupted and collected_emails < target_emails:
            print(f"\n{'='*60}")
            print(f"PAGE {page} | DB Total: {writer.get_total_count()} | Session: {collected_emails}")
            print(f"{'='*60}")

            try:
                profile_links = collect_profile_links(driver)
            except TimeoutException:
                print("  Could not find profile cards on this page; stopping.")
                break
            except Exception as exc:
                print(f"  Unexpected error collecting links: {exc}")
                break

            if not profile_links:
                print("  No profiles found on this page; stopping.")
                break

            # Filter out already-scraped profiles (check database)
            new_links = [link for link in profile_links if not writer.is_scraped(link)]
            skipped = len(profile_links) - len(new_links)
            print(f"  Found {len(profile_links)} profiles, {len(new_links)} new (skipping {skipped} already in DB)")
            
            if not new_links:
                print("  All profiles on this page already in database.")
                # Try to go to next page anyway
                if not click_next_page(driver):
                    print("  No more pages available; stopping.")
                    break
                page += 1
                continue

            # Scrape each new profile
            for i, link in enumerate(new_links):
                if _interrupted or collected_emails >= target_emails:
                    break
                
                try:
                    data = scrape_profile(driver, link, include_all_emails)
                    if data["email"]:
                        inserted = writer.write_row(
                            link,  # profile_url for duplicate checking
                            data["name"], data["email"], data["linkedin"],
                            data["city"], data["state"], data["industry"],
                            data["work_title"], data["firm_name"], data["class_year"]
                        )
                        if inserted:
                            collected_emails += 1
                            extras = []
                            if data["class_year"]:
                                extras.append(f"Class: {data['class_year']}")
                            if data["city"]:
                                extras.append(f"{data['city']}, {data['state']}")
                            if data["work_title"]:
                                extras.append(f"{data['work_title'][:20]}...")
                            if data["linkedin"]:
                                extras.append("LinkedIn: ✓")
                            extra_info = f" | {', '.join(extras)}" if extras else ""
                            total = writer.get_total_count()
                            print(f"    ✓ {data['name']}: {data['email']}{extra_info} [DB: {total}]")
                    else:
                        print(f"    ⚠ No email found for {link.split('/')[-1]}")
                except WebDriverException as exc:
                    print(f"  Failed on {link}: {exc}")
                except Exception as exc:
                    print(f"  Unexpected error on {link}: {exc}")
                
                # Go back to the listing page (browser back preserves pagination state)
                driver.back()
                time.sleep(1.5)  # Wait for page to reload
            
            # Now click Next to go to the next page
            print(f"\n  Clicking Next to go to page {page + 1}...")
            if not click_next_page(driver):
                print("  No more pages available; stopping.")
                break
            page += 1
                    
    except KeyboardInterrupt:
        print("\n\n⚠️  Keyboard interrupt detected. Saving progress...")
    except Exception as exc:
        print(f"\n\n⚠️  Unexpected error: {exc}. Saving progress...")
    finally:
        # Only close if not already closed by signal handler
        if _writer_instance is not None:
            try:
                total = writer.get_total_count()
                writer.close()
                _writer_instance = None  # Clear reference
                print(f"\n💾 Database saved: {total} total records in {db_path}")
                print(f"   (Added {collected_emails} new profiles this session)")
            except Exception:
                pass
        try:
            driver.quit()
        except Exception:
            pass




def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape TigerNet profiles for emails, class year, location, and LinkedIn.")
    
    # Export command (mutually exclusive with scraping)
    parser.add_argument("--export", action="store_true",
                        help="Export database to CSV file instead of scraping.")
    parser.add_argument("--export-file", default="alumni_export.csv",
                        help="CSV file path for export (default: alumni_export.csv)")
    
    # Database options
    parser.add_argument("--db", default="alumni.db",
                        help="SQLite database file (default: alumni.db)")
    
    # Scraping options
    parser.add_argument("--target-emails", type=int, default=None,
                        help="How many emails to collect (1 to 130423). If omitted, you will be prompted.")
    parser.add_argument("--include-all-emails", action="store_true",
                        help="If set, collect all emails on a profile (default: only primary).")
    parser.add_argument("--start-page", type=int, default=START_PAGE,
                        help="Listing page to start from (default 1).")
    parser.add_argument("--end-page", type=int, default=END_PAGE,
                        help="Last page to visit; leave empty for auto-stop.")
    parser.add_argument("--headless", action="store_true",
                        help="Run browser headless once login/profile selectors are confirmed.")
    
    # Legacy option (ignored, kept for backward compatibility)
    parser.add_argument("--output", default=None,
                        help="(Deprecated) Use --db for database path or --export-file for CSV export.")
    
    return parser.parse_args()


if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    
    args = parse_args()

    # Handle export mode
    if args.export:
        if not os.path.exists(args.db):
            raise SystemExit(f"Database not found: {args.db}")
        print(f"📤 Exporting {args.db} to {args.export_file}...")
        export_to_csv(args.db, args.export_file)
        sys.exit(0)

    # Scraping mode
    if args.target_emails is None:
        try:
            user_input = input(f"How many emails to collect (1-{MAX_USERS})? ").strip()
            args.target_emails = int(user_input)
        except Exception:
            raise SystemExit("Please enter a valid integer for target emails.")

    if not (1 <= args.target_emails <= MAX_USERS):
        raise SystemExit(f"--target-emails must be between 1 and {MAX_USERS}")

    START_PAGE = args.start_page
    END_PAGE = args.end_page
    HEADLESS = args.headless or HEADLESS

    print("🚀 Starting TigerNet Scraper...")
    print("📊 Collecting: Name, Email, LinkedIn, City, State, Industry, Work Title, Firm Name, Class Year")
    if FILTERS:
        print(f"🎓 Filter applied: Class Years 1975-2005")
    else:
        print("📋 No filter applied (scraping all alumni)")
    print(f"💾 Database: {args.db}")
    print(f"🎯 Target: {args.target_emails} emails")
    print("⏹  Press Ctrl+C at any time to save progress and exit.\n")
    
    try:
        scrape_directory(args.db, args.include_all_emails, args.target_emails)
        print("\n✅ Scraping completed successfully!")
        print(f"\n💡 To export to CSV, run: python scrape.py --export --db {args.db}")
    except SystemExit:
        # Already handled by signal handler
        pass
    except Exception as exc:
        print(f"\n⚠️  Error during scraping: {exc}")
        print(f"Progress has been saved to {args.db}")
        raise
