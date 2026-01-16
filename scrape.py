import argparse #imports = tools for the program
import csv
import os
import random
import re
import signal
import sys
import time
from typing import Iterable, List, Optional, Sequence, Tuple

from selenium import webdriver #selenium webdriver controls a Chrome browser periodically
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Configuration

BASE_URL = "https://tigernet.princeton.edu/people" # base url to scrape
START_PAGE = 1                      # people?page=<n>; page 1 works without param
END_PAGE = None                     # set to an int to cap pages
HEADLESS = False                    # set True after you verify login works
PAGE_TIMEOUT = 18
DELAY_MIN = 2.2
DELAY_MAX = 5.2
MAX_USERS = 130_423

# Regular Expressions (reg exs)
EMAIL_PATTERNS = [
    re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", re.IGNORECASE),
    re.compile(r"[a-zA-Z0-9._%+-]+[\s@]+[a-zA-Z0-9.-]+[\s.]+[a-zA-Z]{2,}", re.IGNORECASE),
    re.compile(r"[a-zA-Z0-9._%+-]+(?:\s*(?:at|@|&#64;)\s*)[a-zA-Z0-9.-]+(?:\s*(?:dot|\.|\[dot\])\s*)[a-zA-Z]{2,}", re.IGNORECASE),
]

PHONE_PATTERN = re.compile(r"\+?\d[\d\-\s().]{6,}\d")

# Global state for graceful shutdown
_writer_instance = None
_interrupted = False


class IncrementalCSVWriter:
    """CSV writer that saves incrementally and flushes to disk periodically."""
    
    def __init__(self, path: str, include_phone: bool, flush_interval: int = 5):
        self.path = path
        self.include_phone = include_phone
        self.file = open(path, "w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file, delimiter="\t")
        self.row_count = 0
        self.flush_interval = flush_interval
        self.pending_flush = 0
        
        # Write header
        header = ["Name", "Email(s)"]
        if include_phone:
            header.append("Phone(s)")
        self.writer.writerow(header)
        self._flush_to_disk()
    
    def write_row(self, name: str, emails: List[str], phones: List[str]) -> None:
        """Write a single row and flush periodically or on demand."""
        row = [name, ", ".join(emails)]
        if self.include_phone:
            row.append(", ".join(phones))
        self.writer.writerow(row)
        self.row_count += 1
        self.pending_flush += 1
        
        # Flush every N rows for better performance, or immediately on first write for safety
        if self.pending_flush >= self.flush_interval or self.row_count == 1:
            self._flush_to_disk()
            self.pending_flush = 0
    
    def _flush_to_disk(self) -> None:
        """Force flush buffer and sync to disk."""
        self.file.flush()
        try:
            os.fsync(self.file.fileno())
        except (OSError, AttributeError):
            # fsync may not be available on all platforms/file types
            pass
    
    def close(self) -> None:
        """Close the file."""
        if self.file:
            self.file.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) and SIGTERM gracefully."""
    global _interrupted, _writer_instance
    _interrupted = True
    print("\n\n⚠️  Interrupt received. Saving progress...")
    if _writer_instance:
        try:
            _writer_instance._flush_to_disk()
            _writer_instance.close()
        except Exception:
            pass
    print("Progress saved. Exiting...")
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

# extract phone numbers from html
def extract_phones(text: str) -> List[str]:
    """Extract phone numbers using pre-compiled regex pattern."""
    seen = set()  # Use set for O(1) lookups
    for match in PHONE_PATTERN.findall(text):
        cleaned = re.sub(r"[^\d+]", "", match)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
    return list(seen)

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
    """Collect profile links efficiently with optimized scrolling."""
    # Wait for initial content
    wait = WebDriverWait(driver, PAGE_TIMEOUT)
    wait.until(
        EC.presence_of_all_elements_located(
            (By.XPATH, "//a[contains(., 'Go to profile') and contains(@href, '/users/')]")
        )
    )
    
    # Optimized scrolling: scroll faster and wait for dynamic content
    for i in range(2):  # Reduced from 3 to 2
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.3)  # Reduced from 0.6 to 0.3
    
    # Use set for faster duplicate checking
    links_set = set()
    anchors = driver.find_elements(By.XPATH, "//a[contains(., 'Go to profile') and contains(@href, '/users/')]")
    for anchor in anchors:
        href = anchor.get_attribute("href")
        if href and "/users/" in href:
            clean_href = href.split("?", 1)[0]
            if clean_href not in links_set:
                links_set.add(clean_href)
    return list(links_set)


def scrape_profile(driver: webdriver.Chrome, url: str, include_all: bool, include_phone: bool) -> Tuple[str, List[str], List[str]]:
    """Scrape a profile using direct navigation (much faster than opening new windows)."""
    # Store original URL to return to listing page
    original_url = driver.current_url
    
    name_text = "(unknown)"
    emails: List[str] = []
    phones: List[str] = []
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
            return name_text, emails, phones

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
                        if lowered not in ("princeton information", "contact") and text not in name_candidates:
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
            name_text = name_candidates[0]

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

        # Extract phone numbers if requested
        if include_phone:
            phone_blocks = driver.find_elements(By.CSS_SELECTOR, "[data-testid*='phone'], a[href^='tel:']")
            phone_candidates: List[str] = []
            seen_phones = set()
            for block in phone_blocks:
                href = (block.get_attribute("href") or "").strip()
                text = (block.text or "").strip()
                if href.startswith("tel:"):
                    phone_candidates.append(href.replace("tel:", ""))
                if text:
                    phone_candidates.append(text)
            
            for candidate in phone_candidates:
                for extracted in extract_phones(candidate):
                    if extracted and extracted not in seen_phones:
                        phones.append(extracted)
                        seen_phones.add(extracted)

    except WebDriverException as exc:
        print(f"  Error scraping profile {url}: {exc}")
    finally:
        # Return to listing page (delay happens after navigation)
        try:
            driver.get(original_url)
            # Brief wait for page to load, then random delay to avoid rate limiting
            time.sleep(0.3 + random.uniform(DELAY_MIN, DELAY_MAX))
        except WebDriverException:
            # If navigation fails, still add delay
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
    return name_text, emails, phones


def iter_pages() -> Iterable[int]:
    if END_PAGE is None:
        page = START_PAGE
        while True:
            yield page
            page += 1
    else:
        for page in range(START_PAGE, END_PAGE + 1):
            yield page


def scrape_directory(output_path: str, include_all_emails: bool, include_phone: bool, target_emails: int) -> None:
    """Scrape directory and save incrementally to file."""
    global _writer_instance, _interrupted
    
    collected_emails = 0
    driver = build_driver()
    
    # Create incremental writer
    writer = IncrementalCSVWriter(output_path, include_phone)
    _writer_instance = writer  # Store reference for signal handler
    
    try:
        driver.get(BASE_URL)
        input("Log in manually, apply any filters, then press Enter to start scraping...")

        for page in iter_pages():
            if _interrupted or collected_emails >= target_emails:
                break

            page_url = f"{BASE_URL}?page={page}" if page > 1 else BASE_URL
            print(f"Scraping listing page {page} → {page_url}")
            
            try:
                driver.get(page_url)
            except WebDriverException as exc:
                print(f"  Error loading page: {exc}")
                break

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

            print(f"  Found {len(profile_links)} profiles on this page")
            for link in profile_links:
                if _interrupted or collected_emails >= target_emails:
                    break
                try:
                    name, emails, phones = scrape_profile(driver, link, include_all_emails, include_phone)
                    if emails:
                        writer.write_row(name, emails, phones)
                        collected_emails += len(emails)
                        print(f"    ✓ {name}: {', '.join(emails)} [{writer.row_count} rows saved]")
                    else:
                        print(f"    ⚠ No email found for {link}")
                except WebDriverException as exc:
                    print(f"  Failed on {link}: {exc}")
                except Exception as exc:
                    print(f"  Unexpected error on {link}: {exc}")
                    # Continue with next profile even on unexpected errors
                    
    except KeyboardInterrupt:
        print("\n\n⚠️  Keyboard interrupt detected. Saving progress...")
    except Exception as exc:
        print(f"\n\n⚠️  Unexpected error: {exc}. Saving progress...")
    finally:
        writer.close()
        _writer_instance = None  # Clear reference
        try:
            driver.quit()
        except Exception:
            pass
        print(f"\n✓ Saved {writer.row_count} profiles to {output_path}")




def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape TigerNet profiles for emails.")
    parser.add_argument("--target-emails", type=int, default=None,
                        help="How many emails to collect (1 to 130423). If omitted, you will be prompted.")
    parser.add_argument("--include-all-emails", action="store_true",
                        help="If set, collect all emails on a profile (default: only primary).")
    parser.add_argument("--include-phone", action="store_true",
                        help="If set, collect phone numbers when present.")
    parser.add_argument("--start-page", type=int, default=START_PAGE,
                        help="Listing page to start from (default 1).")
    parser.add_argument("--end-page", type=int, default=END_PAGE,
                        help="Last page to visit; leave empty for auto-stop.")
    parser.add_argument("--headless", action="store_true",
                        help="Run browser headless once login/profile selectors are confirmed.")
    parser.add_argument("--output", default="emails.hsv",
                        help="Path to save TSV/HSV output (default emails.hsv).")
    return parser.parse_args()


if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    
    args = parse_args()

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

    print("Starting Selenium directory email scraper...")
    print(f"Data will be saved incrementally to: {args.output}")
    print("Press Ctrl+C at any time to save progress and exit.\n")
    
    try:
        scrape_directory(args.output, args.include_all_emails, args.include_phone, args.target_emails)
        print("\n✓ Scraping completed successfully!")
    except SystemExit:
        # Already handled by signal handler
        pass
    except Exception as exc:
        print(f"\n⚠️  Error during scraping: {exc}")
        print("Progress has been saved to the output file.")
        raise
