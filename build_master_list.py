#!/usr/bin/env python3
"""
Quick fix: Resume building master list from district 17 onwards.
Handles the finicky site by waiting properly for dropdowns.
Run this instead of build_master_list.py to complete the remaining districts.
"""

import time
import sys
import os
import json
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    NoSuchElementException, StaleElementReferenceException
)
import pandas as pd

BASE_URL = "https://registration.ap.gov.in/igrs/ppProperty"


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
    options.page_load_strategy = "none"
    return webdriver.Chrome(options=options)


def wait_for_district_options(driver, timeout=60):
    """Wait until district dropdown has real options."""
    print("  Waiting for district dropdown to populate...", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        try:
            select_el = driver.find_element(By.NAME, "district")
            sel = Select(select_el)
            real = [o for o in sel.options if "SELECT" not in o.text.upper()]
            if len(real) > 0:
                print(f"  ✓ District dropdown ready ({len(real)} districts) after {int(time.time()-start)}s", flush=True)
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def get_dropdown_options(driver, name):
    try:
        select_el = driver.find_element(By.NAME, name)
        select = Select(select_el)
        return [{"text": o.text.strip(), "value": o.get_attribute("value")}
                for o in select.options if o.text.strip() and "SELECT" not in o.text.upper()]
    except Exception as e:
        print(f"    Error reading '{name}': {e}", flush=True)
        return []


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


def main():
    output_dir = "./data"
    headless = "--headless" in sys.argv

    os.makedirs(output_dir, exist_ok=True)

    # Load progress
    progress_file = os.path.join(output_dir, "master_list_progress_rural.json")
    master_records = []
    completed_districts = set()

    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            progress = json.load(f)
            master_records = progress.get("records", [])
            completed_districts = set(progress.get("completed_districts", []))
            print(f"  Resuming: {len(completed_districts)} districts done, {len(master_records)} records", flush=True)
    else:
        print("  No progress file found. Starting fresh.", flush=True)

    print(f"  Headless: {headless}", flush=True)
    print("=" * 60, flush=True)

    # Process one district at a time with a FRESH browser each time
    # This prevents the stale dropdown bug
    districts_to_process = None

    # First, get the full list of districts
    print("\n  Getting district list...", flush=True)
    driver = create_driver(headless=headless)
    try:
        driver.get(BASE_URL)
        time.sleep(3)

        # Wait for form
        for _ in range(30):
            try:
                selects = driver.find_elements(By.TAG_NAME, "select")
                if selects:
                    time.sleep(2)
                    break
            except Exception:
                pass
            time.sleep(2)

        # Click Rural — use JavaScript and verify
        for attempt in range(10):
            try:
                radio = driver.find_element(By.ID, "agri")
                radio.click()
                time.sleep(1)
                driver.execute_script("""
                    var radio = document.getElementById('agri');
                    if (radio) {
                        radio.checked = true;
                        radio.click();
                        radio.dispatchEvent(new Event('change', {bubbles: true}));
                    }
                """)
                time.sleep(2)
                page_text = driver.find_element(By.TAG_NAME, "body").text
                if "Survey No" in page_text:
                    print(f"  ✓ Rural confirmed (attempt {attempt+1})", flush=True)
                    break
                else:
                    print(f"  Rural not confirmed yet (attempt {attempt+1}), retrying...", flush=True)
                    time.sleep(2)
            except Exception:
                time.sleep(2)

        # Wait for district options
        if not wait_for_district_options(driver):
            print("  ✗ District dropdown empty. Retrying with page reload...", flush=True)
            driver.get(BASE_URL)
            time.sleep(5)
            try:
                driver.find_element(By.ID, "agri").click()
                time.sleep(1)
            except Exception:
                pass
            if not wait_for_district_options(driver):
                print("  ✗ Still empty. Check VPN and try again.", flush=True)
                driver.quit()
                return

        districts_to_process = get_dropdown_options(driver, "district")
        print(f"  Found {len(districts_to_process)} total districts", flush=True)
    finally:
        driver.quit()

    # Now process each district with a FRESH browser
    for d_idx, dist in enumerate(districts_to_process, 1):
        dist_name = dist["text"]
        dist_value = dist["value"]

        if dist_name in completed_districts:
            print(f"\n  [{d_idx}/{len(districts_to_process)}] {dist_name} — already done, skipping", flush=True)
            continue

        print(f"\n  [{d_idx}/{len(districts_to_process)}] District: {dist_name}", flush=True)
        print(f"  {'─' * 50}", flush=True)

        # Fresh browser for each district!
        driver = create_driver(headless=headless)
        try:
            driver.get(BASE_URL)
            time.sleep(3)

            # Wait for form
            for _ in range(30):
                try:
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    if selects:
                        time.sleep(2)
                        break
                except Exception:
                    pass
                time.sleep(2)

            # Click Rural — use JavaScript and verify by checking page text
            for attempt in range(10):
                try:
                    # Try regular click first
                    radio = driver.find_element(By.ID, "agri")
                    radio.click()
                    time.sleep(1)
                    
                    # Also try JavaScript click
                    driver.execute_script("""
                        var radio = document.getElementById('agri');
                        if (radio) {
                            radio.checked = true;
                            radio.click();
                            radio.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    """)
                    time.sleep(2)
                    
                    # Verify by checking if "Survey No" text appears (Rural) vs "Door No" (Urban)
                    page_text = driver.find_element(By.TAG_NAME, "body").text
                    if "Survey No" in page_text:
                        print(f"    ✓ Rural confirmed (attempt {attempt+1})", flush=True)
                        break
                    else:
                        print(f"    Rural not confirmed yet (attempt {attempt+1}), retrying...", flush=True)
                        time.sleep(2)
                except Exception as e:
                    print(f"    Radio click error (attempt {attempt+1}): {e}", flush=True)
                    time.sleep(2)

            # Wait for district options
            if not wait_for_district_options(driver):
                print(f"    ✗ Districts didn't load, skipping {dist_name}", flush=True)
                driver.quit()
                continue

            # Select district
            select_with_workaround(driver, "district", dist_value, dist_name)
            time.sleep(3)

            # Wait for mandal
            if not wait_for_dropdown_populated(driver, "Mandal", timeout=20):
                print(f"    Mandal didn't populate, retrying district...", flush=True)
                select_with_workaround(driver, "district", dist_value, dist_name)
                time.sleep(5)
                if not wait_for_dropdown_populated(driver, "Mandal", timeout=20):
                    print(f"    ✗ Mandal still empty, skipping {dist_name}", flush=True)
                    driver.quit()
                    continue

            mandals = get_dropdown_options(driver, "Mandal")
            print(f"    Found {len(mandals)} mandals", flush=True)

            for m_idx, mand in enumerate(mandals, 1):
                mand_name = mand["text"]
                mand_value = mand["value"]

                print(f"    [{m_idx}/{len(mandals)}] Mandal: {mand_name}", end="", flush=True)

                select_with_workaround(driver, "Mandal", mand_value, mand_name)
                time.sleep(3)

                if not wait_for_dropdown_populated(driver, "Village", timeout=15):
                    print(f" — no villages", flush=True)
                    select_with_workaround(driver, "Mandal", mand_value, mand_name)
                    time.sleep(4)
                    if not wait_for_dropdown_populated(driver, "Village", timeout=15):
                        print(f"      ✗ Village dropdown empty, skipping", flush=True)
                        continue

                villages = get_dropdown_options(driver, "Village")
                print(f" — {len(villages)} villages", flush=True)

                for village in villages:
                    master_records.append({
                        "district": dist_name,
                        "district_value": dist_value,
                        "mandal": mand_name,
                        "mandal_value": mand_value,
                        "village": village["text"],
                        "village_value": village["value"],
                        "property_type": "rural",
                    })

                time.sleep(0.5)

            # Mark district complete and save
            completed_districts.add(dist_name)
            with open(progress_file, "w") as f:
                json.dump({
                    "records": master_records,
                    "completed_districts": list(completed_districts),
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }, f)
            print(f"    ✓ Progress saved ({len(master_records)} total records)", flush=True)

        except Exception as e:
            print(f"    ✗ Error: {e}", flush=True)
            # Save progress anyway
            with open(progress_file, "w") as f:
                json.dump({
                    "records": master_records,
                    "completed_districts": list(completed_districts),
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }, f)
        finally:
            driver.quit()
            print(f"    Browser closed for {dist_name}", flush=True)

        time.sleep(2)

    # Save final output
    if master_records:
        df = pd.DataFrame(master_records)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(output_dir, f"master_list_rural_{timestamp}.csv")
        xlsx_path = os.path.join(output_dir, f"master_list_rural_{timestamp}.xlsx")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"\n  ✓ CSV saved: {csv_path}", flush=True)
        try:
            df.to_excel(xlsx_path, index=False, engine="openpyxl")
            print(f"  ✓ Excel saved: {xlsx_path}", flush=True)
        except Exception:
            pass

        print(f"\n{'=' * 60}", flush=True)
        print(f"  SUMMARY", flush=True)
        print(f"{'=' * 60}", flush=True)
        print(f"  Total Districts: {df['district'].nunique()}", flush=True)
        print(f"  Total Mandals:   {df[['district','mandal']].drop_duplicates().shape[0]}", flush=True)
        print(f"  Total Villages:  {len(df)}", flush=True)
        print(f"{'=' * 60}", flush=True)

        for dist_name, group in df.groupby("district"):
            n_mandals = group["mandal"].nunique()
            n_villages = len(group)
            print(f"    {dist_name}: {n_mandals} mandals, {n_villages} villages", flush=True)

    # Clean up progress
    if os.path.exists(progress_file):
        os.remove(progress_file)

    print("\n  Done!", flush=True)


if __name__ == "__main__":
    main()