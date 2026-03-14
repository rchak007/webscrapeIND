#!/usr/bin/env python3
"""
Parallel Scrape All - AP Registration Prohibited Properties
=============================================================
Runs multiple scrapers in parallel, each handling different districts.
Uses the same scrape_progress.json as scrape_all.py — fully compatible.

Usage:
    python scrape_all_parallel.py --master-list ./data/master_list_rural_20260221_101737.csv --output-dir ./data --workers 4
    python scrape_all_parallel.py --master-list ./data/master_list_rural_20260221_101737.csv --output-dir ./data --workers 4 --headless

    # Check progress (same as scrape_all.py)
    python scrape_all.py --status --output-dir ./data
"""

import argparse
import time
import sys
import os
import re
import json
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    print("ERROR: selenium not installed. Run: pip install selenium", flush=True)
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl", flush=True)
    sys.exit(1)


BASE_URL = "https://registration.ap.gov.in/igrs/ppProperty"
DELAY_BETWEEN_REQUESTS = 2
MAX_RETRIES_PER_LOCATION = 2
BROWSER_RESTART_INTERVAL = 50

# Thread-safe lock for progress file
progress_lock = threading.Lock()


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
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors=yes")
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
    try:
        select_el = driver.find_element(By.NAME, name)
        select = Select(select_el)
        try:
            select.select_by_value(value)
            return True
        except Exception:
            pass
        if text:
            try:
                select.select_by_visible_text(text)
                return True
            except Exception:
                pass
        driver.execute_script("""
            var select = arguments[0];
            for (var i = 0; i < select.options.length; i++) {
                if (select.options[i].value === arguments[1]) {
                    select.selectedIndex = i;
                    select.dispatchEvent(new Event('change', {bubbles: true}));
                    break;
                }
            }
        """, select_el, value)
        return True
    except Exception:
        return False


def select_with_workaround(driver, name, value, text=""):
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
                           village, village_value, property_type="rural", worker_id=0):
    """Scrape one location. Returns (data_list, success_bool, error_message)."""
    tag = f"[W{worker_id}]"
    try:
        print(f"{tag}   [1] Loading page...", flush=True)
        driver.get(BASE_URL)
        if not wait_for_form(driver):
            return [], False, "Page failed to load"
        print(f"{tag}   [1] ✓ Page loaded", flush=True)

        # Select Rural — JavaScript click + verify
        radio_id = "agri" if property_type == "rural" else "nonAgri"
        verify_text = "Survey No" if property_type == "rural" else "Door No"
        rural_confirmed = False
        for attempt in range(10):
            try:
                radio = driver.find_element(By.ID, radio_id)
                radio.click()
                time.sleep(1)
                driver.execute_script(f"""
                    var radio = document.getElementById('{radio_id}');
                    if (radio) {{
                        radio.checked = true;
                        radio.click();
                        radio.dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                """)
                time.sleep(2)
                page_text = driver.find_element(By.TAG_NAME, "body").text
                if verify_text in page_text:
                    rural_confirmed = True
                    break
                if attempt >= 3:
                    print(f"{tag}   [2] {property_type} not confirmed (attempt {attempt+1}), retrying...", flush=True)
                time.sleep(1)
            except Exception:
                time.sleep(2)

        if rural_confirmed:
            print(f"{tag}   [2] ✓ {property_type} selected", flush=True)
        else:
            print(f"{tag}   [2] ⚠ Could not verify {property_type} after 10 attempts", flush=True)

        # Wait for district dropdown
        print(f"{tag}   [3] Selecting district: {district}", flush=True)
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
        print(f"{tag}   [3] ✓ District selected", flush=True)
        time.sleep(3)

        # Wait for mandal
        print(f"{tag}   [4] Selecting mandal: {mandal}", flush=True)
        if not wait_for_dropdown_populated(driver, "Mandal", timeout=20):
            print(f"{tag}   [4] Mandal dropdown empty, retrying district...", flush=True)
            select_with_workaround(driver, "district", district_value, district)
            time.sleep(5)
            if not wait_for_dropdown_populated(driver, "Mandal", timeout=20):
                return [], False, f"Mandal dropdown empty for {district}"

        # Select mandal
        if not select_with_workaround(driver, "Mandal", mandal_value, mandal):
            return [], False, f"Could not select mandal: {mandal}"
        print(f"{tag}   [4] ✓ Mandal selected", flush=True)
        time.sleep(3)

        # Wait for village
        print(f"{tag}   [5] Selecting village: {village}", flush=True)
        if not wait_for_dropdown_populated(driver, "Village", timeout=20):
            print(f"{tag}   [5] Village dropdown empty, retrying mandal...", flush=True)
            select_with_workaround(driver, "Mandal", mandal_value, mandal)
            time.sleep(5)
            if not wait_for_dropdown_populated(driver, "Village", timeout=15):
                return [], False, f"Village dropdown empty for {mandal}"

        # Select village
        if not select_by_value_safe(driver, "Village", village_value, village):
            return [], False, f"Could not select village: {village}"
        print(f"{tag}   [5] ✓ Village selected", flush=True)
        time.sleep(1)

        # Enter survey no = *
        try:
            door_input = driver.find_element(By.ID, "surveyNo")
            door_input.clear()
            door_input.send_keys("*")
            print(f"{tag}   [6] ✓ Entered * in survey no", flush=True)
        except Exception as e:
            print(f"{tag}   [6] ⚠ Could not enter survey no: {e}", flush=True)

        # Click Get Details
        print(f"{tag}   [7] Clicking Get Details...", flush=True)
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
        print(f"{tag}   [7] ✓ Submitted, waiting for results...", flush=True)
        time.sleep(8)

        # Scrape table
        data, headers = scrape_table(driver)
        print(f"{tag}   [8] Scraped: {len(data)} rows", flush=True)
        return data, True, None

    except Exception as e:
        return [], False, str(e)


# ─── Progress Tracking (thread-safe) ─────────────────────────────────────────

def load_progress(progress_file):
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            return json.load(f)
    return {
        "completed": {},
        "failed": {},
        "total_rows": 0,
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def save_progress_safe(progress, progress_file):
    with progress_lock:
        progress["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(progress_file, "w") as f:
            json.dump(progress, f, indent=2)


def location_key(district, mandal, village):
    return f"{district}|{mandal}|{village}"


def is_completed_or_failed(progress, key):
    if key in progress["completed"]:
        return True
    if key in progress["failed"]:
        if progress["failed"][key].get("attempts", 0) >= MAX_RETRIES_PER_LOCATION:
            return True
    return False


# ─── Worker Function ─────────────────────────────────────────────────────────

def worker_scrape_districts(worker_id, districts_chunk, df_master, progress,
                            progress_file, output_dir, headless, property_type,
                            global_stats):
    """
    Each worker handles a list of districts.
    """
    tag = f"[W{worker_id}]"
    driver = None
    locations_since_restart = 0
    worker_scraped = 0
    worker_rows = 0
    worker_errors = 0
    worker_start = time.time()

    # Filter master list to only this worker's districts
    df_worker = df_master[df_master["district"].isin(districts_chunk)]
    worker_total = len(df_worker)

    print(f"\n{tag} Starting — {len(districts_chunk)} districts, {worker_total} locations", flush=True)
    print(f"{tag} Districts: {districts_chunk}", flush=True)

    try:
        for idx, row in df_worker.iterrows():
            district = row["district"]
            mandal = row["mandal"]
            village = row["village"]
            district_value = str(row.get("district_value", ""))
            mandal_value = str(row.get("mandal_value", ""))
            village_value = str(row.get("village_value", ""))

            key = location_key(district, mandal, village)

            # Skip if already done (thread-safe read)
            with progress_lock:
                if is_completed_or_failed(progress, key):
                    continue

            # Create or restart browser
            if driver is None or locations_since_restart >= BROWSER_RESTART_INTERVAL:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(2)
                print(f"{tag} Starting Chrome...", flush=True)
                driver = create_driver(headless=headless)
                locations_since_restart = 0

            # Progress
            with progress_lock:
                total_done = len(progress["completed"])
                total_all = global_stats["total_locations"]
            pct = (total_done / total_all * 100) if total_all > 0 else 0

            elapsed = time.time() - worker_start
            rate = worker_scraped / (elapsed / 60) if elapsed > 60 else 0
            remaining = worker_total - worker_scraped
            eta_str = ""
            if rate > 0:
                eta_mins = remaining / rate
                eta_str = f" | ETA: {eta_mins/60:.1f}h" if eta_mins > 60 else f" | ETA: {eta_mins:.0f}m"

            ts = datetime.now().strftime("%H:%M:%S")
            print(f"\n{tag} [{ts}] ({pct:.1f}% overall{eta_str}) {district} → {mandal} → {village}", flush=True)

            # Scrape
            data, success, error = scrape_single_location(
                driver, district, district_value,
                mandal, mandal_value,
                village, village_value,
                property_type=property_type,
                worker_id=worker_id
            )

            with progress_lock:
                if success and data:
                    for row_data in data:
                        row_data["_district"] = district
                        row_data["_mandal"] = mandal
                        row_data["_village"] = village
                        row_data["_scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    safe_name = re.sub(r'[^\w]', '_', f"{district}_{mandal}_{village}")[:100]
                    village_csv = os.path.join(output_dir, "per_village", f"{safe_name}.csv")
                    df_village = pd.DataFrame(data)
                    df_village.to_csv(village_csv, index=False, encoding="utf-8-sig")

                    progress["completed"][key] = {
                        "rows": len(data),
                        "file": village_csv,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    progress["total_rows"] += len(data)
                    worker_rows += len(data)
                    print(f"{tag} ✓ {len(data)} rows", flush=True)

                elif success and not data:
                    progress["completed"][key] = {
                        "rows": 0,
                        "file": None,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    print(f"{tag} ✓ 0 rows", flush=True)

                else:
                    attempts = progress["failed"].get(key, {}).get("attempts", 0) + 1
                    progress["failed"][key] = {
                        "error": error,
                        "attempts": attempts,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    print(f"{tag} ✗ {error}", flush=True)
                    worker_errors += 1

                    if worker_errors % 5 == 0:
                        print(f"{tag} ⚠ {worker_errors} errors, restarting browser...", flush=True)
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                        locations_since_restart = BROWSER_RESTART_INTERVAL
                        time.sleep(3)

                # Save progress
                save_progress_safe(progress, progress_file)

            worker_scraped += 1
            locations_since_restart += 1

            # Periodic summary
            if worker_scraped % 25 == 0:
                elapsed_str = str(timedelta(seconds=int(time.time() - worker_start)))
                print(f"\n{tag} ═══ CHECKPOINT: {worker_scraped} done, {worker_rows} rows, {worker_errors} errors, elapsed: {elapsed_str} ═══", flush=True)

            time.sleep(DELAY_BETWEEN_REQUESTS)

    except Exception as e:
        print(f"{tag} FATAL ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    elapsed_str = str(timedelta(seconds=int(time.time() - worker_start)))
    print(f"\n{tag} ═══ FINISHED: {worker_scraped} locations, {worker_rows} rows, {worker_errors} errors, time: {elapsed_str} ═══", flush=True)

    return {
        "worker_id": worker_id,
        "scraped": worker_scraped,
        "rows": worker_rows,
        "errors": worker_errors,
    }


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Parallel scraper for AP Registration prohibited properties",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--master-list", type=str, required=True, help="Path to master list CSV/Excel")
    parser.add_argument("--output-dir", type=str, default="./data", help="Output directory")
    parser.add_argument("--headless", action="store_true", help="Run without visible browser")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel browsers (default: 4)")

    args = parser.parse_args()

    if not os.path.exists(args.master_list):
        print(f"ERROR: File not found: {args.master_list}", flush=True)
        sys.exit(1)

    # Read master list
    if args.master_list.endswith(".csv"):
        df_master = pd.read_csv(args.master_list)
    else:
        df_master = pd.read_excel(args.master_list)

    total_locations = len(df_master)
    districts = df_master["district"].unique().tolist()

    # Property type
    property_type = "rural"
    if "property_type" in df_master.columns:
        property_type = df_master["property_type"].iloc[0]

    print("=" * 60, flush=True)
    print("PARALLEL Scrape All Locations", flush=True)
    print("=" * 60, flush=True)
    print(f"  Master list:    {args.master_list}", flush=True)
    print(f"  Total locations: {total_locations}", flush=True)
    print(f"  Total districts: {len(districts)}", flush=True)
    print(f"  Workers:        {args.workers}", flush=True)
    print(f"  Headless:       {args.headless}", flush=True)
    print(f"  Property type:  {property_type}", flush=True)
    print("=" * 60, flush=True)

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "per_village"), exist_ok=True)

    # Load progress
    progress_file = os.path.join(args.output_dir, "scrape_progress.json")
    progress = load_progress(progress_file)

    completed_count = len(progress["completed"])
    failed_count = len(progress["failed"])
    print(f"\n  Previously completed: {completed_count}", flush=True)
    print(f"  Previously failed:    {failed_count}", flush=True)
    print(f"  Remaining:            ~{total_locations - completed_count}", flush=True)

    # Figure out which districts still have work
    districts_with_work = []
    for dist in districts:
        dist_rows = df_master[df_master["district"] == dist]
        remaining = 0
        for _, row in dist_rows.iterrows():
            key = location_key(row["district"], row["mandal"], row["village"])
            if not is_completed_or_failed(progress, key):
                remaining += 1
        if remaining > 0:
            districts_with_work.append((dist, remaining))

    districts_with_work.sort(key=lambda x: -x[1])  # Largest first for better load balancing

    print(f"\n  Districts with remaining work: {len(districts_with_work)}", flush=True)
    for dist, remaining in districts_with_work:
        print(f"    {dist}: {remaining} remaining", flush=True)

    if not districts_with_work:
        print("\n  All locations already completed!", flush=True)
        return

    # Split districts across workers (round-robin for balance)
    num_workers = min(args.workers, len(districts_with_work))
    worker_districts = [[] for _ in range(num_workers)]
    for i, (dist, _) in enumerate(districts_with_work):
        worker_districts[i % num_workers].append(dist)

    print(f"\n  Work distribution across {num_workers} workers:", flush=True)
    for i, dists in enumerate(worker_districts):
        total = sum(r for d, r in districts_with_work if d in dists)
        print(f"    Worker {i}: {len(dists)} districts, ~{total} locations — {dists}", flush=True)

    global_stats = {"total_locations": total_locations}

    # Launch workers
    print(f"\n{'=' * 60}", flush=True)
    print(f"  LAUNCHING {num_workers} WORKERS", flush=True)
    print(f"{'=' * 60}\n", flush=True)

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for i, dists in enumerate(worker_districts):
            future = executor.submit(
                worker_scrape_districts,
                worker_id=i,
                districts_chunk=dists,
                df_master=df_master,
                progress=progress,
                progress_file=progress_file,
                output_dir=args.output_dir,
                headless=args.headless,
                property_type=property_type,
                global_stats=global_stats,
            )
            futures[future] = i

        # Wait for all to complete
        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                result = future.result()
                print(f"\n  Worker {worker_id} completed: {result}", flush=True)
            except Exception as e:
                print(f"\n  Worker {worker_id} crashed: {e}", flush=True)

    # Final save
    save_progress_safe(progress, progress_file)

    # Combine results
    print(f"\n{'=' * 60}", flush=True)
    print("Combining all results...", flush=True)
    print(f"{'=' * 60}", flush=True)

    per_village_dir = os.path.join(args.output_dir, "per_village")
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
            combined_csv = os.path.join(args.output_dir, f"ALL_prohibited_properties_{timestamp}.csv")
            combined.to_csv(combined_csv, index=False, encoding="utf-8-sig")
            print(f"  ✓ Combined CSV: {combined_csv}", flush=True)
            print(f"  Total rows: {len(combined)}", flush=True)

            try:
                combined_xlsx = os.path.join(args.output_dir, f"ALL_prohibited_properties_{timestamp}.xlsx")
                combined.to_excel(combined_xlsx, index=False, engine="openpyxl")
                print(f"  ✓ Combined Excel: {combined_xlsx}", flush=True)
            except Exception:
                pass

    # Final summary
    elapsed_total = time.time() - start_time
    elapsed_str = str(timedelta(seconds=int(elapsed_total)))

    print(f"\n{'=' * 60}", flush=True)
    print(f"  FINAL SUMMARY", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"  Total locations:  {total_locations}", flush=True)
    print(f"  Completed:       {len(progress['completed'])}", flush=True)
    print(f"  Failed:          {len(progress['failed'])}", flush=True)
    print(f"  Total rows:      {progress['total_rows']}", flush=True)
    print(f"  Session time:    {elapsed_str}", flush=True)
    print(f"  Workers used:    {num_workers}", flush=True)
    print(f"{'=' * 60}", flush=True)


if __name__ == "__main__":
    main()