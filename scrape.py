import argparse
import csv
import random
import re
import time
from typing import Iterable, List, Optional, Sequence, Tuple

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ──────────────────────────────────────────────
#           CONFIGURATION - CHANGE THESE
# ──────────────────────────────────────────────

BASE_URL = "https://tigernet.princeton.edu/people"
START_PAGE = 1                      # people?page=<n>; page 1 works without param
END_PAGE = None                     # set to an int to cap pages
HEADLESS = False                    # set True after you verify login works
PAGE_TIMEOUT = 18
DELAY_MIN = 2.2
DELAY_MAX = 5.2
MAX_USERS = 130_423

EMAIL_PATTERNS = [
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    r"[a-zA-Z0-9._%+-]+[\s@]+[a-zA-Z0-9.-]+[\s.]+[a-zA-Z]{2,}",
    r"[a-zA-Z0-9._%+-]+(?:\s*(?:at|@|&#64;)\s*)[a-zA-Z0-9.-]+(?:\s*(?:dot|\.|\[dot\])\s*)[a-zA-Z]{2,}",
]

PHONE_PATTERN = r"\+?\d[\d\-\s().]{6,}\d"


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


def extract_emails_by_regex(text: str) -> List[str]:
    found = []
    for pattern in EMAIL_PATTERNS:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
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


def extract_phones(text: str) -> List[str]:
    seen = []
    for match in re.findall(PHONE_PATTERN, text):
        cleaned = re.sub(r"[^\d+]", "", match)
        if cleaned and cleaned not in seen:
            seen.append(cleaned)
    return seen


def build_driver() -> webdriver.Chrome:
    options = ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-notifications")
    options.add_argument("--start-maximized")
    # Reuse the existing Chrome/Edge profile if you need to stay logged in:
    # options.add_argument("--user-data-dir=/path/to/your/profile")
    return webdriver.Chrome(options=options, service=Service())


def collect_profile_links(driver: webdriver.Chrome) -> List[str]:
    # Scroll a bit to ensure cards load before collecting links
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.6)

    wait = WebDriverWait(driver, PAGE_TIMEOUT)
    wait.until(
        EC.presence_of_all_elements_located(
            (By.XPATH, "//a[contains(., 'Go to profile') and contains(@href, '/users/')]")
        )
    )
    links = []
    anchors = driver.find_elements(By.XPATH, "//a[contains(., 'Go to profile') and contains(@href, '/users/')]")
    for anchor in anchors:
        href = anchor.get_attribute("href")
        if href and "/users/" in href and href not in links:
            links.append(href.split("?", 1)[0])
    return links


def scrape_profile(driver: webdriver.Chrome, url: str, include_all: bool, include_phone: bool) -> Tuple[str, List[str], List[str]]:
    driver.execute_script("window.open(arguments[0], '_blank');", url)
    driver.switch_to.window(driver.window_handles[-1])
    name_text = "(unknown)"
    emails: List[str] = []
    phones: List[str] = []

    try:
        WebDriverWait(driver, PAGE_TIMEOUT).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "[data-testid='display-attribute-email']")
            )
        )
    except TimeoutException:
        print(f"  Timeout waiting for profile {url}")
    else:
        # Name is typically in the page header (use a few fallbacks and ignore sub-card headings)
        candidates = []
        name_selectors = (
            "h1[data-testid='profile-name']",
            "[data-testid='profile-name']",
            "[data-testid='member-name']",
            "h3.sc-braxZu.DwTMa",  # profile heading on many user pages
            "header h1",
            "main h1",
            "h1",
            "h2",
        )
        for selector in name_selectors:
            for elem in driver.find_elements(By.CSS_SELECTOR, selector):
                text = (elem.text or "").strip()
                lowered = text.lower()
                if text and lowered not in ("princeton information", "contact") and text not in candidates:
                    candidates.append(text)

        if not candidates:
            # Fallback: look for first display-attribute-simple-string near "Full Name"
            try:
                full_name_elem = driver.find_element(
                    By.XPATH,
                    "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'full name')]/following::*[@data-testid='display-attribute-simple-string'][1]"
                )
                full_name_text = (full_name_elem.text or "").strip()
                lowered = full_name_text.lower()
                if full_name_text and lowered not in ("princeton information", "contact"):
                    candidates.append(full_name_text)
            except WebDriverException:
                pass

        if candidates:
            name_text = candidates[0]

        email_blocks = driver.find_elements(By.CSS_SELECTOR, "[data-testid='display-attribute-email']")
        emails = extract_emails_from_blocks(email_blocks)

        if not include_all and emails:
            emails = emails[:1]

        if not emails:
            # Fallback to regex scan so we do not silently miss anything
            emails = extract_emails_by_regex(driver.page_source)
            if not include_all and emails:
                emails = emails[:1]

        if include_phone:
            phone_blocks = driver.find_elements(By.CSS_SELECTOR, "[data-testid*='phone'], a[href^='tel:']")
            phone_candidates: List[str] = []
            for block in phone_blocks:
                href = (block.get_attribute("href") or "").strip()
                text = (block.text or "").strip()
                if href.startswith("tel:"):
                    phone_candidates.append(href.replace("tel:", ""))
                if text:
                    phone_candidates.append(text)
            phones = []
            for candidate in phone_candidates:
                for extracted in extract_phones(candidate):
                    if extracted and extracted not in phones:
                        phones.append(extracted)

    driver.close()
    driver.switch_to.window(driver.window_handles[0])
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


def scrape_directory() -> List[Tuple[str, List[str], List[str]]]:
    all_rows: List[Tuple[str, List[str], List[str]]] = []
    collected_emails = 0
    driver = build_driver()

    try:
        driver.get(BASE_URL)
        input("Log in manually, apply any filters, then press Enter to start scraping...")

        for page in iter_pages():
            if collected_emails >= args.target_emails:
                break

            page_url = f"{BASE_URL}?page={page}" if page > 1 else BASE_URL
            print(f"Scraping listing page {page} → {page_url}")
            driver.get(page_url)

            try:
                profile_links = collect_profile_links(driver)
            except TimeoutException:
                print("  Could not find profile cards on this page; stopping.")
                break

            if not profile_links:
                print("  No profiles found on this page; stopping.")
                break

            print(f"  Found {len(profile_links)} profiles on this page")
            for link in profile_links:
                if collected_emails >= args.target_emails:
                    break
                try:
                    name, emails, phones = scrape_profile(driver, link, args.include_all_emails, args.include_phone)
                    if emails:
                        all_rows.append((name, emails, phones))
                        collected_emails += len(emails)
                        print(f"    ✓ {name}: {', '.join(emails)}")
                    else:
                        print(f"    ⚠ No email found for {link}")
                except WebDriverException as exc:
                    print(f"  Failed on {link}: {exc}")

        return all_rows
    finally:
        driver.quit()


def write_hsv(rows: List[Tuple[str, List[str], List[str]]], path: str, include_phone: bool) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        header = ["Name", "Email(s)"]
        if include_phone:
            header.append("Phone(s)")
        writer.writerow(header)

        for name, emails, phones in rows:
            row = [name, ", ".join(emails)]
            if include_phone:
                row.append(", ".join(phones))
            writer.writerow(row)


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

    print("Starting Selenium directory email scraper...\n")
    rows = scrape_directory()
    total_emails = sum(len(emails) for _, emails, _ in rows)

    print(f"\nCollected {len(rows)} profiles and {total_emails} emails:\n")
    for name, emails, _ in rows:
        print(f"{name}: {', '.join(emails)}")

    write_hsv(rows, args.output, args.include_phone)
    print(f"\nSaved to {args.output}")
