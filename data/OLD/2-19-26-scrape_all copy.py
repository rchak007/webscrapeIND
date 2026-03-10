#!/usr/bin/env python3
"""
Scrape All Locations - AP Registration Prohibited Properties
=============================================================
Reads the master list Excel and scrapes every district/mandal/village combo.
Has full resume capability — if interrupted, just run again.

Usage:
    python scrape_all.py --master-list ./data/master_list_urban_20260218_031009.xlsx --output-dir ./data
    python scrape_all.py --master-list ./data/master_list_urban_20260218_031009.xlsx --output-dir ./data --headless
    python scrape_all.py --master-list ./data/master_list_urban_20260218_031009.xlsx --output-dir ./data --start-from 500
    python scrape_all.py --status --output-dir ./data   # Check progress without scraping
"""

import argparse
import time
import sys
import os
import re
import json
from datetime import datetime, timedelta

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import (
        NoSuchElementException, StaleElementReferenceException,
        ElementClickInterceptedException
    )
except ImportError:
    print("ERROR: selenium not installed. Run: pip install selenium")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)


BASE_URL = "https://registration.ap.gov.in/igrs/ppProperty"

# How many locations to scrape before restarting the browser (prevents memory leaks)
BROWSER_RESTART_INTERVAL = 50

# Delay between requests (be nice to the server)
DELAY_BETWEEN_REQUESTS = 3

# Max retries per location
MAX_RETRIES_PER_LOCATION = 2


# ─── Browser & Selenium Helpers ──────────────────────────────────────────────

def create_driver(headless=False):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.page_load_strategy = "none"

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    return driver


def wait_for_form(driver, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            selects = driver.find_elements(By.TAG_NAME, "select")
            if len(selects) >= 1:
                time.sleep(2)
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def wait_for_dropdown_populated(driver, name, timeout=20):
    start = time.time()
    while time.time() - start < timeout:
        try:
            select_el = driver.find_element(By.NAME, name)
            options = select_el.find_elements(By.TAG_NAME, "option")
            real = [o for o in options if "SELECT" not in o.text.upper()]
            if len(real) > 0:
                return True
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        time.sleep(1)
    return False


def select_by_value_safe(driver, name, value, text=""):
    """Select dropdown option with multiple fallback methods."""
    try:
        select_el = driver.find_element(By.NAME, name)
        select = Select(select_el)

        # Method 1: by value
        try:
            select.select_by_value(value)
            return True
        except Exception:
            pass

        # Method 2: by visible text
        if text:
            try:
                select.select_by_visible_text(text)
                return True
            except Exception:
                pass

        # Method 3: by index
        for idx, option in enumerate(select.options):
            if option.get_attribute("value") == value:
                select.select_by_index(idx)
                return True

        # Method 4: JavaScript
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
            select_el, value
        )
        return True

    except Exception:
        return False


def select_with_workaround(driver, name, value, text=""):
    """Select with the finicky-site workaround."""
    try:
        select_el = driver.find_element(By.NAME, name)
        select = Select(select_el)
        other = None
        for o in select.options:
            v = o.get_attribute("value")
            if v and v != value and "SELECT" not in o.text.upper():
                other = v
                break
        if other:
            select_by_value_safe(driver, name, other)
            time.sleep(1.5)
        return select_by_value_safe(driver, name, value, text)
    except Exception:
        return False


def scrape_table(driver):
    """Scrape the results table."""
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


# ─── Single Location Scrape ──────────────────────────────────────────────────

def scrape_single_location(driver, district, district_value, mandal, mandal_value,
                           village, village_value, property_type="urban"):
    """
    Scrape one location using an already-open browser.
    Returns (data_list, success_bool, error_message).
    """
    try:
        # Load page
        driver.get(BASE_URL)
        if not wait_for_form(driver):
            return [], False, "Page failed to load"

        # Select property type
        radio_id = "agri" if property_type == "rural" else "nonAgri"
        try:
            driver.find_element(By.ID, radio_id).click()
            time.sleep(1)
        except Exception:
            pass

        # Wait for district dropdown
        for _ in range(5):
            try:
                sel = Select(driver.find_element(By.NAME, "district"))
                real = [o for o in sel.options if "SELECT" not in o.text.upper()]
                if real:
                    break
            except Exception:
                pass
            time.sleep(2)

        # Select district
        if not select_with_workaround(driver, "district", district_value, district):
            return [], False, f"Could not select district: {district}"
        time.sleep(3)

        # Wait for mandal
        if not wait_for_dropdown_populated(driver, "Mandal", timeout=20):
            # Retry district
            select_with_workaround(driver, "district", district_value, district)
            time.sleep(5)
            if not wait_for_dropdown_populated(driver, "Mandal", timeout=20):
                return [], False, f"Mandal dropdown empty for {district}"

        # Select mandal
        if not select_with_workaround(driver, "Mandal", mandal_value, mandal):
            return [], False, f"Could not select mandal: {mandal}"
        time.sleep(3)

        # Wait for village
        if not wait_for_dropdown_populated(driver, "Village", timeout=20):
            select_with_workaround(driver, "Mandal", mandal_value, mandal)
            time.sleep(5)
            if not wait_for_dropdown_populated(driver, "Village", timeout=15):
                return [], False, f"Village dropdown empty for {mandal}"

        # Select village
        if not select_by_value_safe(driver, "Village", village_value, village):
            return [], False, f"Could not select village: {village}"
        time.sleep(1)

        # Enter door no = *
        try:
            door_input = driver.find_element(By.ID, "surveyNo")
            door_input.clear()
            door_input.send_keys("*")
        except Exception:
            pass

        # Click Get Details
        try:
            submit_btn = driver.find_element(
                By.XPATH,
                "//button[contains(text(),'Get Details')] | //input[@value='Get Details'] | //button[@type='submit'] | //input[@type='submit']"
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", submit_btn)
            time.sleep(0.5)
            submit_btn.click()
        except ElementClickInterceptedException:
            try:
                submit_btn = driver.find_element(
                    By.XPATH,
                    "//button[contains(text(),'Get Details')] | //button[@type='submit']"
                )
                driver.execute_script("arguments[0].click();", submit_btn)
            except Exception as e:
                return [], False, f"Could not click submit: {e}"
        except Exception as e:
            return [], False, f"Could not find submit button: {e}"

        # Wait for results
        time.sleep(8)

        # Scrape table
        data, headers = scrape_table(driver)

        return data, True, None

    except Exception as e:
        return [], False, str(e)


# ─── Progress Tracking ───────────────────────────────────────────────────────

def load_progress(progress_file):
    """Load progress from JSON file."""
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            return json.load(f)
    return {
        "completed": {},      # key: "district|mandal|village" → {"rows": N, "file": "path", "timestamp": "..."}
        "failed": {},         # key: "district|mandal|village" → {"error": "...", "attempts": N, "timestamp": "..."}
        "total_rows": 0,
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def save_progress(progress, progress_file):
    """Save progress to JSON file."""
    progress["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(progress_file, "w") as f:
        json.dump(progress, f, indent=2)


def location_key(district, mandal, village):
    """Create a unique key for a location."""
    return f"{district}|{mandal}|{village}"


# ─── Main Scrape All ─────────────────────────────────────────────────────────

def scrape_all(master_list_path, output_dir="./data", headless=False, start_from=0, max_locations=None):
    """
    Read master list and scrape all locations.
    """
    # Read master list
    if master_list_path.endswith(".csv"):
        df_master = pd.read_csv(master_list_path)
    else:
        df_master = pd.read_excel(master_list_path)

    total_locations = len(df_master)

    print("=" * 60)
    print("Scrape All Locations")
    print("=" * 60)
    print(f"  Master list:   {master_list_path}")
    print(f"  Total locations: {total_locations}")
    print(f"  Output dir:    {output_dir}")
    print(f"  Headless:      {headless}")
    if start_from > 0:
        print(f"  Starting from: #{start_from}")
    if max_locations:
        print(f"  Max locations: {max_locations}")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "per_village"), exist_ok=True)

    # Load progress
    progress_file = os.path.join(output_dir, "scrape_progress.json")
    progress = load_progress(progress_file)

    completed_count = len(progress["completed"])
    failed_count = len(progress["failed"])
    print(f"\n  Previously completed: {completed_count}")
    print(f"  Previously failed:    {failed_count}")
    print(f"  Remaining:            {total_locations - completed_count}")

    # Determine property type from master list
    property_type = "urban"
    if "property_type" in df_master.columns:
        property_type = df_master["property_type"].iloc[0]

    # Stats tracking
    scrape_start_time = time.time()
    session_scraped = 0
    session_rows = 0
    session_errors = 0

    driver = None
    locations_since_restart = 0

    try:
        for idx, row in df_master.iterrows():
            # Skip rows before start_from
            if idx < start_from:
                continue

            # Check max_locations limit
            if max_locations and session_scraped >= max_locations:
                print(f"\n  Reached max_locations limit ({max_locations}). Stopping.")
                break

            district = row["district"]
            mandal = row["mandal"]
            village = row["village"]
            district_value = str(row.get("district_value", ""))
            mandal_value = str(row.get("mandal_value", ""))
            village_value = str(row.get("village_value", ""))

            key = location_key(district, mandal, village)

            # Skip if already completed
            if key in progress["completed"]:
                continue

            # Skip if already failed too many times
            if key in progress["failed"]:
                if progress["failed"][key].get("attempts", 0) >= MAX_RETRIES_PER_LOCATION:
                    continue

            # Create or restart browser
            if driver is None or locations_since_restart >= BROWSER_RESTART_INTERVAL:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    print(f"\n  Restarting browser (every {BROWSER_RESTART_INTERVAL} locations)...")
                    time.sleep(2)
                driver = create_driver(headless=headless)
                locations_since_restart = 0

            # Progress display
            total_done = len(progress["completed"])
            pct = (total_done / total_locations) * 100 if total_locations > 0 else 0
            elapsed = time.time() - scrape_start_time
            rate = session_scraped / (elapsed / 60) if elapsed > 60 else 0
            eta_mins = ((total_locations - total_done) / rate) if rate > 0 else 0

            eta_str = ""
            if rate > 0:
                if eta_mins > 60:
                    eta_str = f" | ETA: {eta_mins/60:.1f} hours"
                else:
                    eta_str = f" | ETA: {eta_mins:.0f} min"

            print(f"\n  [{total_done + 1}/{total_locations}] ({pct:.1f}%{eta_str})")
            print(f"  {district} → {mandal} → {village}")

            # Scrape
            data, success, error = scrape_single_location(
                driver, district, district_value,
                mandal, mandal_value,
                village, village_value,
                property_type=property_type
            )

            if success and data:
                # Add metadata
                for row_data in data:
                    row_data["_district"] = district
                    row_data["_mandal"] = mandal
                    row_data["_village"] = village
                    row_data["_scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Save per-village CSV
                safe_name = re.sub(r'[^\w]', '_', f"{district}_{mandal}_{village}")[:100]
                village_csv = os.path.join(output_dir, "per_village", f"{safe_name}.csv")
                df_village = pd.DataFrame(data)
                df_village.to_csv(village_csv, index=False, encoding="utf-8-sig")

                # Update progress
                progress["completed"][key] = {
                    "rows": len(data),
                    "file": village_csv,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                progress["total_rows"] += len(data)

                print(f"  ✓ {len(data)} rows → {village_csv}")

                session_rows += len(data)

            elif success and not data:
                # No data for this location (not an error)
                progress["completed"][key] = {
                    "rows": 0,
                    "file": None,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                print(f"  ✓ No prohibited properties (0 rows)")

            else:
                # Error
                attempts = progress["failed"].get(key, {}).get("attempts", 0) + 1
                progress["failed"][key] = {
                    "error": error,
                    "attempts": attempts,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                print(f"  ✗ Error: {error}")
                session_errors += 1

                # If too many consecutive errors, restart browser
                if session_errors % 5 == 0:
                    print("  Too many errors, restarting browser...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = None
                    locations_since_restart = BROWSER_RESTART_INTERVAL  # Force restart
                    time.sleep(5)

            # Save progress every location
            save_progress(progress, progress_file)

            session_scraped += 1
            locations_since_restart += 1

            # Delay between requests
            time.sleep(DELAY_BETWEEN_REQUESTS)

        # ─── Combine all per-village CSVs ─────────────────────────────

        print(f"\n{'=' * 60}")
        print("Combining all results...")
        print(f"{'=' * 60}")

        per_village_dir = os.path.join(output_dir, "per_village")
        csv_files = [os.path.join(per_village_dir, f) for f in os.listdir(per_village_dir) if f.endswith(".csv")]

        if csv_files:
            all_dfs = []
            for f in csv_files:
                try:
                    df = pd.read_csv(f, encoding="utf-8-sig")
                    if len(df) > 0:
                        all_dfs.append(df)
                except Exception:
                    pass

            if all_dfs:
                combined = pd.concat(all_dfs, ignore_index=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                combined_csv = os.path.join(output_dir, f"ALL_prohibited_properties_{timestamp}.csv")
                combined_xlsx = os.path.join(output_dir, f"ALL_prohibited_properties_{timestamp}.xlsx")

                combined.to_csv(combined_csv, index=False, encoding="utf-8-sig")
                print(f"  ✓ Combined CSV:   {combined_csv}")

                try:
                    combined.to_excel(combined_xlsx, index=False, engine="openpyxl")
                    print(f"  ✓ Combined Excel: {combined_xlsx}")
                except Exception as e:
                    print(f"  WARNING: Could not save combined Excel: {e}")

                print(f"  Total combined rows: {len(combined)}")

        # ─── Final Summary ────────────────────────────────────────────

        elapsed_total = time.time() - scrape_start_time
        elapsed_str = str(timedelta(seconds=int(elapsed_total)))

        print(f"\n{'=' * 60}")
        print(f"  FINAL SUMMARY")
        print(f"{'=' * 60}")
        print(f"  Total locations in master list: {total_locations}")
        print(f"  Completed:   {len(progress['completed'])}")
        print(f"  Failed:      {len(progress['failed'])}")
        print(f"  Remaining:   {total_locations - len(progress['completed']) - len([k for k,v in progress['failed'].items() if v.get('attempts',0) >= MAX_RETRIES_PER_LOCATION])}")
        print(f"  Total rows:  {progress['total_rows']}")
        print(f"  Session time: {elapsed_str}")
        print(f"  Session scraped: {session_scraped} locations, {session_rows} rows")
        if session_scraped > 0:
            avg = elapsed_total / session_scraped
            print(f"  Avg per location: {avg:.1f}s")
        print(f"{'=' * 60}")

    except KeyboardInterrupt:
        print(f"\n\n  ⚠ Interrupted! Progress saved.")
        print(f"  Run the same command again to resume.")
        save_progress(progress, progress_file)

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()
        save_progress(progress, progress_file)

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        print("\n  Browser closed.")


# ─── Status Check ────────────────────────────────────────────────────────────

def show_status(output_dir):
    """Show scraping progress without running the scraper."""
    progress_file = os.path.join(output_dir, "scrape_progress.json")

    if not os.path.exists(progress_file):
        print("No progress file found. Scraping hasn't started yet.")
        return

    with open(progress_file, "r") as f:
        progress = json.load(f)

    completed = progress.get("completed", {})
    failed = progress.get("failed", {})

    print(f"{'=' * 60}")
    print(f"  SCRAPING STATUS")
    print(f"{'=' * 60}")
    print(f"  Started:       {progress.get('started_at', 'unknown')}")
    print(f"  Last updated:  {progress.get('last_updated', 'unknown')}")
    print(f"  Completed:     {len(completed)} locations")
    print(f"  Failed:        {len(failed)} locations")
    print(f"  Total rows:    {progress.get('total_rows', 0)}")

    # Completed by district
    if completed:
        district_counts = {}
        for key, info in completed.items():
            dist = key.split("|")[0]
            if dist not in district_counts:
                district_counts[dist] = {"locations": 0, "rows": 0}
            district_counts[dist]["locations"] += 1
            district_counts[dist]["rows"] += info.get("rows", 0)

        print(f"\n  Per-District Progress:")
        for dist, counts in sorted(district_counts.items()):
            print(f"    {dist}: {counts['locations']} locations, {counts['rows']} rows")

    # Failed locations
    if failed:
        print(f"\n  Failed Locations (last 10):")
        for key, info in list(failed.items())[-10:]:
            parts = key.split("|")
            print(f"    {parts[0]} → {parts[1]} → {parts[2]}")
            print(f"      Error: {info.get('error', 'unknown')}")
            print(f"      Attempts: {info.get('attempts', 0)}")

    print(f"{'=' * 60}")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape all locations from master list",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run the full scrape
  python scrape_all.py --master-list ./data/master_list_urban_20260218_031009.xlsx --output-dir ./data

  # Run headless
  python scrape_all.py --master-list ./data/master_list_urban_20260218_031009.xlsx --output-dir ./data --headless

  # Start from a specific row (e.g., skip first 500)
  python scrape_all.py --master-list ./data/master_list_urban_20260218_031009.xlsx --output-dir ./data --start-from 500

  # Limit to 100 locations (for testing)
  python scrape_all.py --master-list ./data/master_list_urban_20260218_031009.xlsx --output-dir ./data --max 100

  # Check progress
  python scrape_all.py --status --output-dir ./data
        """
    )

    parser.add_argument("--master-list", type=str, help="Path to master list CSV/Excel")
    parser.add_argument("--output-dir", type=str, default="./data", help="Output directory")
    parser.add_argument("--headless", action="store_true", help="Run without visible browser")
    parser.add_argument("--start-from", type=int, default=0, help="Start from row number (0-indexed)")
    parser.add_argument("--max", type=int, default=None, help="Max locations to scrape (for testing)")
    parser.add_argument("--status", action="store_true", help="Show progress status and exit")

    args = parser.parse_args()

    if args.status:
        show_status(args.output_dir)
        return

    if not args.master_list:
        print("ERROR: --master-list is required")
        print("Usage: python scrape_all.py --master-list ./data/master_list.xlsx --output-dir ./data")
        sys.exit(1)

    if not os.path.exists(args.master_list):
        print(f"ERROR: File not found: {args.master_list}")
        sys.exit(1)

    scrape_all(
        master_list_path=args.master_list,
        output_dir=args.output_dir,
        headless=args.headless,
        start_from=args.start_from,
        max_locations=args.max,
    )


if __name__ == "__main__":
    main()