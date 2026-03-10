#!/usr/bin/env python3
"""
AP Registration & Stamps Department - Prohibited Properties Scraper v2
======================================================================
Scrapes prohibited property data from registration.ap.gov.in/igrs/ppProperty

Usage:
    python scrape_prohibited_properties.py
    python scrape_prohibited_properties.py --district "KAKINADA" --mandal "KAKINADA (URBAN)" --village "Kakinada(Urban)"
    python scrape_prohibited_properties.py --property-type rural --district "KAKINADA" --mandal "KAKINADA" --village "SomeVillage"
    python scrape_prohibited_properties.py --headless
"""

import argparse
import time
import sys
import os
import re
from datetime import datetime

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException,
        StaleElementReferenceException, ElementClickInterceptedException
    )
except ImportError:
    print("ERROR: selenium not installed. Run: pip install selenium")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)


# ─── Configuration ───────────────────────────────────────────────────────────

BASE_URL = "https://registration.ap.gov.in/igrs/ppProperty"

# Element selectors (based on debug inspection)
SELECTORS = {
    "radio_urban":   (By.ID, "nonAgri"),
    "radio_rural":   (By.ID, "agri"),
    "district":      (By.NAME, "district"),
    "mandal":        (By.NAME, "Mandal"),
    "village":       (By.NAME, "Village"),
    "door_no":       (By.ID, "surveyNo"),
    "submit":        (By.XPATH, "//button[contains(text(),'Get Details')] | //input[@value='Get Details'] | //button[@type='submit'] | //input[@type='submit']"),
}


# ─── Helper Functions ────────────────────────────────────────────────────────

def create_driver(headless=False):
    """Create and configure Chrome WebDriver."""
    options = Options()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    # Don't wait for full page load (site is slow)
    options.page_load_strategy = "none"

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            from selenium.webdriver.chrome.service import Service
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except ImportError:
            print("ERROR: Could not start Chrome. Try: pip install webdriver-manager")
            sys.exit(1)

    return driver


def wait_for_form(driver, timeout=60):
    """Wait for the form to appear on the page."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            selects = driver.find_elements(By.TAG_NAME, "select")
            if len(selects) >= 1:
                time.sleep(2)  # Let remaining elements settle
                return True
        except Exception:
            pass
        time.sleep(2)
        elapsed = int(time.time() - start)
        if elapsed % 10 == 0:
            print(f"    Still waiting... ({elapsed}s)")
    return False


def select_dropdown_with_retry(driver, locator, value, max_retries=3, wait_after=2, use_workaround=True):
    """
    Select a dropdown value with retry logic.
    The site is finicky — sometimes you need to select something else first,
    then reselect the desired value to trigger the AJAX cascade.
    
    use_workaround: If True, selects a different option first to force AJAX.
                    Only needed for district dropdown. Set False for mandal/village.
    """
    by, selector = locator

    for attempt in range(1, max_retries + 1):
        try:
            # Re-find element fresh each attempt (React may re-render)
            select_el = driver.find_element(by, selector)
            select = Select(select_el)

            # Print available options for debugging
            all_opts = [(o.text.strip(), o.get_attribute("value")) for o in select.options if "SELECT" not in o.text.upper()]
            if attempt == 1:
                display = [t for t, v in all_opts[:8]]
                print(f"    Available options ({len(all_opts)}): {display}{'...' if len(all_opts) > 8 else ''}")

            # Find matching option text and value (case-insensitive)
            target_text = None
            target_value = None
            for option in select.options:
                if option.text.strip().upper() == value.strip().upper():
                    target_text = option.text.strip()
                    target_value = option.get_attribute("value")
                    break

            # Try partial match if exact fails
            if not target_text:
                for option in select.options:
                    if value.strip().upper() in option.text.strip().upper():
                        target_text = option.text.strip()
                        target_value = option.get_attribute("value")
                        break

            if not target_text:
                if attempt < max_retries:
                    print(f"    Value '{value}' not found (attempt {attempt}/{max_retries}), retrying...")
                    time.sleep(3)
                    continue
                else:
                    print(f"  ✗ Value '{value}' not found in dropdown.")
                    print(f"    Available: {[t for t, v in all_opts]}")
                    return False

            # WORKAROUND for finicky site (district/mandal dropdown):
            if use_workaround:
                other_options = [(o.text.strip(), o.get_attribute("value")) for o in select.options
                                 if o.text.strip() != target_text
                                 and "SELECT" not in o.text.upper()
                                 and o.text.strip() != ""]
                if other_options:
                    try:
                        select.select_by_value(other_options[0][1])
                    except Exception:
                        select.select_by_visible_text(other_options[0][0])
                    print(f"    (workaround: temp selected '{other_options[0][0]}')")
                    time.sleep(2)
                    # Re-find the element (React re-renders)
                    select_el = driver.find_element(by, selector)
                    select = Select(select_el)
                    # Re-find target value after re-render
                    for option in select.options:
                        if option.text.strip().upper() == value.strip().upper():
                            target_value = option.get_attribute("value")
                            target_text = option.text.strip()
                            break

            # Try multiple selection methods
            selected = False

            # Method 1: select by value (most reliable with React)
            if target_value and not selected:
                try:
                    select.select_by_value(target_value)
                    selected = True
                    print(f"  ✓ Selected '{target_text}' (by value={target_value})")
                except Exception:
                    pass

            # Method 2: select by visible text
            if not selected:
                try:
                    select.select_by_visible_text(target_text)
                    selected = True
                    print(f"  ✓ Selected '{target_text}' (by text)")
                except Exception:
                    pass

            # Method 3: select by index
            if not selected:
                try:
                    for idx, option in enumerate(select.options):
                        if option.text.strip().upper() == value.strip().upper():
                            select.select_by_index(idx)
                            selected = True
                            print(f"  ✓ Selected '{target_text}' (by index={idx})")
                            break
                except Exception:
                    pass

            # Method 4: JavaScript as last resort
            if not selected:
                try:
                    driver.execute_script(
                        """
                        var select = arguments[0];
                        for (var i = 0; i < select.options.length; i++) {
                            if (select.options[i].value === arguments[1]) {
                                select.selectedIndex = i;
                                select.dispatchEvent(new Event('change', {bubbles: true}));
                                break;
                            }
                        }
                        """,
                        select_el, target_value
                    )
                    selected = True
                    print(f"  ✓ Selected '{target_text}' (via JavaScript)")
                except Exception as js_err:
                    print(f"    JS fallback also failed: {js_err}")

            if selected:
                time.sleep(wait_after)
                return True
            else:
                print(f"    All selection methods failed (attempt {attempt}/{max_retries})")
                time.sleep(3)

        except StaleElementReferenceException:
            print(f"    Stale element (attempt {attempt}/{max_retries}), retrying...")
            time.sleep(3)
        except Exception as e:
            print(f"    Error (attempt {attempt}/{max_retries}): {e}")
            time.sleep(3)

    return False


def wait_for_dropdown_populated(driver, locator, timeout=15):
    """Wait until a dropdown has more than just the 'SELECT...' option."""
    by, selector = locator
    start = time.time()

    while time.time() - start < timeout:
        try:
            select_el = driver.find_element(by, selector)
            options = select_el.find_elements(By.TAG_NAME, "option")
            real_options = [o for o in options if "SELECT" not in o.text.upper()]
            if len(real_options) > 0:
                return True
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        time.sleep(1)

    return False


def scrape_table(driver):
    """Scrape the results table from the page."""
    time.sleep(3)

    tables = driver.find_elements(By.TAG_NAME, "table")

    data_table = None
    for t in tables:
        rows = t.find_elements(By.TAG_NAME, "tr")
        if len(rows) > 1:
            first_row = rows[0]
            headers = first_row.find_elements(By.TAG_NAME, "th")
            if not headers:
                headers = first_row.find_elements(By.TAG_NAME, "td")
            if len(headers) >= 3:
                data_table = t
                break

    if not data_table:
        for css in ["table", ".table", "[class*='table']", "[class*='Table']"]:
            try:
                candidates = driver.find_elements(By.CSS_SELECTOR, css)
                for c in candidates:
                    rows = c.find_elements(By.TAG_NAME, "tr")
                    if len(rows) > 2:
                        data_table = c
                        break
                if data_table:
                    break
            except Exception:
                continue

    if not data_table:
        print("  WARNING: No data table found on page.")
        return [], []

    rows = data_table.find_elements(By.TAG_NAME, "tr")

    header_row = rows[0]
    header_cells = header_row.find_elements(By.TAG_NAME, "th")
    if not header_cells:
        header_cells = header_row.find_elements(By.TAG_NAME, "td")
    headers = [cell.text.strip() for cell in header_cells]

    data = []
    for row in rows[1:]:
        cells = row.find_elements(By.TAG_NAME, "td")
        if cells:
            row_data = {}
            for i, cell in enumerate(cells):
                col_name = headers[i] if i < len(headers) else f"Column_{i}"
                row_data[col_name] = cell.text.strip()
            if any(v for v in row_data.values()):
                data.append(row_data)

    return data, headers


def check_and_scrape_all_pages(driver):
    """Handle pagination if present."""
    all_data = []
    headers = []
    page_num = 1

    while True:
        print(f"  Scraping page {page_num}...")
        page_data, page_headers = scrape_table(driver)

        if not headers and page_headers:
            headers = page_headers

        if page_data:
            all_data.extend(page_data)
            print(f"    → {len(page_data)} rows found")
        else:
            if page_num == 1:
                print("    → No data found")
            break

        # Look for pagination
        next_clicked = False
        for selector in [
            "a[aria-label='Next']",
            "li.next a",
            ".pagination .next a",
            "[class*='next']",
            "[class*='Next']",
        ]:
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, selector)
                if next_btn.is_displayed() and next_btn.is_enabled():
                    next_btn.click()
                    time.sleep(3)
                    page_num += 1
                    next_clicked = True
                    break
            except (NoSuchElementException, ElementClickInterceptedException):
                continue

        if not next_clicked:
            try:
                page_links = driver.find_elements(By.CSS_SELECTOR, ".pagination a, [class*='page'] a")
                for link in page_links:
                    if link.text.strip() == str(page_num + 1):
                        link.click()
                        time.sleep(3)
                        page_num += 1
                        next_clicked = True
                        break
            except Exception:
                pass

        if not next_clicked:
            break

    return all_data, headers


# ─── Main Scraping Logic ─────────────────────────────────────────────────────

def scrape_prohibited_properties(
    district, mandal, village,
    door_no="*",
    property_type="urban",
    headless=False,
    output_dir="."
):
    print("=" * 60)
    print("AP Registration - Prohibited Properties Scraper v2")
    print("=" * 60)
    print(f"  District:      {district}")
    print(f"  Mandal:        {mandal}")
    print(f"  Village:       {village}")
    print(f"  Door No:       {door_no}")
    print(f"  Property Type: {property_type}")
    print(f"  Headless:      {headless}")
    print("=" * 60)

    driver = create_driver(headless=headless)

    try:
        # Step 1: Load page
        print("\n[1/6] Loading page...")
        driver.get(BASE_URL)

        if not wait_for_form(driver):
            print("  ✗ Page failed to load. Check your VPN connection.")
            driver.save_screenshot(os.path.join(output_dir, "error_page_load.png"))
            return None
        print("  ✓ Page loaded")

        # Step 2: Select property type
        print(f"\n[2/6] Selecting {property_type} properties...")
        try:
            if property_type.lower() == "rural":
                radio = driver.find_element(*SELECTORS["radio_rural"])
            else:
                radio = driver.find_element(*SELECTORS["radio_urban"])
            radio.click()
            time.sleep(1)
            print(f"  ✓ Selected {property_type}")
        except Exception as e:
            print(f"  WARNING: Could not click radio button: {e}")
            print("  Proceeding with default (Urban)...")

        # Step 3: Select District
        print(f"\n[3/6] Selecting district: {district}")
        if not select_dropdown_with_retry(driver, SELECTORS["district"], district, wait_after=4, use_workaround=True):
            print("  ✗ Failed to select district. Exiting.")
            driver.save_screenshot(os.path.join(output_dir, "error_district.png"))
            return None

        # Step 4: Select Mandal (wait for it to populate after district selection)
        print(f"\n[4/6] Selecting mandal: {mandal}")
        print("  Waiting for mandal dropdown to populate...")
        if not wait_for_dropdown_populated(driver, SELECTORS["mandal"], timeout=20):
            print("  WARNING: Mandal dropdown didn't populate. Retrying district selection...")
            select_dropdown_with_retry(driver, SELECTORS["district"], district, wait_after=5, use_workaround=True)
            if not wait_for_dropdown_populated(driver, SELECTORS["mandal"], timeout=20):
                print("  ✗ Mandal dropdown still empty. The site may be having issues.")
                driver.save_screenshot(os.path.join(output_dir, "error_mandal_empty.png"))
                return None

        if not select_dropdown_with_retry(driver, SELECTORS["mandal"], mandal, wait_after=4, use_workaround=True):
            print("  ✗ Failed to select mandal. Exiting.")
            driver.save_screenshot(os.path.join(output_dir, "error_mandal.png"))
            return None

        # Step 5: Select Village (wait for it to populate)
        print(f"\n[5/6] Selecting village: {village}")
        print("  Waiting for village dropdown to populate...")
        if not wait_for_dropdown_populated(driver, SELECTORS["village"], timeout=20):
            print("  WARNING: Village dropdown didn't populate. Retrying mandal selection...")
            select_dropdown_with_retry(driver, SELECTORS["mandal"], mandal, wait_after=5, use_workaround=True)
            if not wait_for_dropdown_populated(driver, SELECTORS["village"], timeout=20):
                print("  ✗ Village dropdown still empty.")
                driver.save_screenshot(os.path.join(output_dir, "error_village_empty.png"))
                return None

        if not select_dropdown_with_retry(driver, SELECTORS["village"], village, wait_after=2, use_workaround=False):
            print("  ✗ Failed to select village. Exiting.")
            driver.save_screenshot(os.path.join(output_dir, "error_village.png"))
            return None

        # Step 5b: Enter Door No
        print(f"\n  Entering Door No: {door_no}")
        try:
            door_input = driver.find_element(*SELECTORS["door_no"])
            door_input.clear()
            door_input.send_keys(door_no)
            print(f"  ✓ Entered Door No: {door_no}")
        except Exception as e:
            print(f"  WARNING: Could not enter door no: {e}")

        # Step 6: Click "Get Details"
        print("\n[6/6] Clicking 'Get Details'...")
        try:
            submit_btn = driver.find_element(*SELECTORS["submit"])
            driver.execute_script("arguments[0].scrollIntoView(true);", submit_btn)
            time.sleep(0.5)
            submit_btn.click()
            print("  ✓ Form submitted")
        except ElementClickInterceptedException:
            submit_btn = driver.find_element(*SELECTORS["submit"])
            driver.execute_script("arguments[0].click();", submit_btn)
            print("  ✓ Form submitted (via JS click)")
        except Exception as e:
            print(f"  ✗ Could not click submit: {e}")
            driver.save_screenshot(os.path.join(output_dir, "error_submit.png"))
            return None

        # Wait for results
        print("\n  Waiting for results to load...")
        time.sleep(8)

        # Screenshot of results
        driver.save_screenshot(os.path.join(output_dir, "results_screenshot.png"))
        print("  Screenshot saved: results_screenshot.png")

        # Scrape results
        print("\n  Scraping results table...")
        all_data, headers = check_and_scrape_all_pages(driver)

        if not all_data:
            print("\n  ✗ No data found. Possible reasons:")
            print("    - No prohibited properties for this location")
            print("    - Page didn't load results properly")
            print("    - Check results_screenshot.png")
            try:
                body_text = driver.find_element(By.TAG_NAME, "body").text
                if "no record" in body_text.lower() or "no data" in body_text.lower():
                    print("    - Site says: No records found")
                with open(os.path.join(output_dir, "debug_results_page.html"), "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                print("    - Page source saved to: debug_results_page.html")
            except Exception:
                pass
            return None

        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        df["_district"] = district
        df["_mandal"] = mandal
        df["_village"] = village
        df["_scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save files
        safe_name = re.sub(r'[^\w]', '_', f"{district}_{mandal}_{village}").strip('_')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        csv_path = os.path.join(output_dir, f"prohibited_{safe_name}_{timestamp}.csv")
        xlsx_path = os.path.join(output_dir, f"prohibited_{safe_name}_{timestamp}.xlsx")

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"\n  ✓ CSV saved:   {csv_path}")

        try:
            df.to_excel(xlsx_path, index=False, engine="openpyxl")
            print(f"  ✓ Excel saved: {xlsx_path}")
        except Exception as e:
            print(f"  WARNING: Could not save Excel: {e}")

        print(f"\n  Total records: {len(df)}")
        print(f"\n  Columns: {list(df.columns)}")
        print(f"\n  Preview (first 5 rows):")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 200)
        print(df.head().to_string())

        return df

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()
        try:
            driver.save_screenshot(os.path.join(output_dir, "error_screenshot.png"))
        except Exception:
            pass
        return None

    finally:
        driver.quit()
        print("\n  Browser closed.")


# ─── Batch scraping ──────────────────────────────────────────────────────────

def scrape_batch(locations, headless=False, output_dir="."):
    """Scrape multiple locations."""
    all_dfs = []
    for i, loc in enumerate(locations, 1):
        print(f"\n{'#' * 60}")
        print(f"# Location {i}/{len(locations)}")
        print(f"{'#' * 60}")

        df = scrape_prohibited_properties(
            district=loc["district"],
            mandal=loc["mandal"],
            village=loc["village"],
            door_no=loc.get("door_no", "*"),
            property_type=loc.get("property_type", "urban"),
            headless=headless,
            output_dir=output_dir
        )
        if df is not None:
            all_dfs.append(df)
        if i < len(locations):
            print("\n  Waiting 5s before next request...")
            time.sleep(5)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(output_dir, f"prohibited_COMBINED_{timestamp}.xlsx")
        combined.to_excel(path, index=False, engine="openpyxl")
        print(f"\n\n✓ Combined file: {path}  ({len(combined)} total records)")

    return all_dfs


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Prohibited Properties from AP Registration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape_prohibited_properties.py --district "KAKINADA" --mandal "KAKINADA (URBAN)" --village "Kakinada(Urban)"
  python scrape_prohibited_properties.py --district "KAKINADA" --mandal "KAKINADA (URBAN)" --village "Kakinada(Urban)" --headless
  python scrape_prohibited_properties.py --property-type rural --district "GUNTUR" --mandal "GUNTUR" --village "Guntur"
        """
    )

    parser.add_argument("--district", type=str, help="District name (e.g., 'KAKINADA')")
    parser.add_argument("--mandal", type=str, help="Mandal name (e.g., 'KAKINADA (URBAN)')")
    parser.add_argument("--village", type=str, help="Village name (e.g., 'Kakinada(Urban)')")
    parser.add_argument("--door-no", type=str, default="*", help="Door number or '*' for all (default: *)")
    parser.add_argument("--property-type", type=str, default="urban", choices=["urban", "rural"])
    parser.add_argument("--headless", action="store_true", help="Run without visible browser")
    parser.add_argument("--output-dir", type=str, default=".", help="Output directory")

    args = parser.parse_args()

    if not all([args.district, args.mandal, args.village]):
        print("Running in interactive mode.\n")
        district = input("Enter District (e.g., KAKINADA): ").strip()
        mandal = input("Enter Mandal (e.g., KAKINADA (URBAN)): ").strip()
        village = input("Enter Village (e.g., Kakinada(Urban)): ").strip()
        door_no = input("Enter Door No (or * for all) [*]: ").strip() or "*"
        prop_type = input("Property type (urban/rural) [urban]: ").strip() or "urban"
        headless_input = input("Run headless? (y/n) [n]: ").strip().lower()
        headless = headless_input == "y"
    else:
        district = args.district
        mandal = args.mandal
        village = args.village
        door_no = args.door_no
        prop_type = args.property_type
        headless = args.headless

    os.makedirs(args.output_dir, exist_ok=True)

    scrape_prohibited_properties(
        district=district,
        mandal=mandal,
        village=village,
        door_no=door_no,
        property_type=prop_type,
        headless=headless,
        output_dir=args.output_dir
    )


if __name__ == "__main__":
    main()