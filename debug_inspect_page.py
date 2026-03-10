#!/usr/bin/env python3
"""
Debug script v2 - Handles the finicky AP Registration site.
Correct URL: https://registration.ap.gov.in/igrs/ppProperty
"""

import time
import sys

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import Select, WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    print("ERROR: selenium not installed. Run: pip install selenium")
    sys.exit(1)

URL = "https://registration.ap.gov.in/igrs/ppProperty"

options = Options()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
# Don't set a strict page load timeout — let it load slowly
options.page_load_strategy = "none"  # Don't wait for full page load

driver = webdriver.Chrome(options=options)

try:
    print(f"Loading: {URL}")
    print("(Waiting up to 60 seconds for page to load...)\n")
    driver.get(URL)

    # Instead of page load timeout, poll for the form to appear
    max_wait = 60
    start = time.time()
    form_found = False

    while time.time() - start < max_wait:
        try:
            # Look for ANY select element or the heading text
            selects = driver.find_elements(By.TAG_NAME, "select")
            if len(selects) >= 1:
                form_found = True
                print(f"✓ Form detected after {int(time.time()-start)}s ({len(selects)} dropdowns found)")
                time.sleep(3)  # Let remaining elements load
                break
        except:
            pass
        time.sleep(2)
        elapsed = int(time.time() - start)
        print(f"  Waiting... ({elapsed}s)")

    if not form_found:
        print("✗ Form not found after 60s. Saving what we have...")
        driver.save_screenshot("debug_screenshot.png")
        with open("debug_page_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("  Saved debug_screenshot.png and debug_page_source.html")
        print(f"  Current URL: {driver.current_url}")
        print(f"  Page title: {driver.title}")
        sys.exit(1)

    # Save page source
    with open("debug_page_source.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print("✓ Page source saved to: debug_page_source.html")

    driver.save_screenshot("debug_screenshot.png")
    print("✓ Screenshot saved to: debug_screenshot.png\n")

    # ─── Find ALL radio buttons ─────────────────────────────────────
    print("=" * 60)
    print("RADIO BUTTONS:")
    print("=" * 60)
    radios = driver.find_elements(By.CSS_SELECTOR, "input[type='radio']")
    if not radios:
        print("  None found. Checking for Angular/React radio components...")
        # Check for material/bootstrap radio buttons
        radio_like = driver.find_elements(By.CSS_SELECTOR, "[role='radio'], .form-check-input, .custom-control-input, mat-radio-button")
        for r in radio_like:
            print(f"  Tag: {r.tag_name}  Class: {r.get_attribute('class')}  Text: {r.text}")
    else:
        for r in radios:
            rid = r.get_attribute("id")
            rname = r.get_attribute("name")
            rvalue = r.get_attribute("value")
            checked = r.is_selected()
            label_text = ""
            try:
                label = driver.find_element(By.CSS_SELECTOR, f"label[for='{rid}']")
                label_text = label.text
            except:
                try:
                    parent = r.find_element(By.XPATH, "./..")
                    label_text = parent.text
                except:
                    pass
            print(f"  ID: {rid}  Name: {rname}  Value: {rvalue}  Checked: {checked}  Label: '{label_text}'")
    print()

    # ─── Find ALL select/dropdown elements ───────────────────────────
    print("=" * 60)
    print("DROPDOWNS (SELECT elements):")
    print("=" * 60)
    selects = driver.find_elements(By.TAG_NAME, "select")
    if not selects:
        print("  No <select> elements found.")
        print("  Checking for custom dropdowns (Angular/React)...")
        custom = driver.find_elements(By.CSS_SELECTOR, "[role='listbox'], [role='combobox'], .dropdown, .select, ng-select, mat-select")
        for c in custom:
            print(f"  Tag: {c.tag_name}  ID: {c.get_attribute('id')}  Class: {c.get_attribute('class')}")
    else:
        for s in selects:
            sid = s.get_attribute("id")
            sname = s.get_attribute("name")
            sclass = s.get_attribute("class")
            sel = Select(s)
            options_list = [o.text.strip() for o in sel.options[:15]]
            total = len(sel.options)
            print(f"  ID:      {sid}")
            print(f"  Name:    {sname}")
            print(f"  Class:   {sclass}")
            print(f"  Options: {total} total")
            print(f"  Values:  {options_list}")
            print()

    # ─── Find ALL input fields ───────────────────────────────────────
    print("=" * 60)
    print("INPUT FIELDS (text/number/search):")
    print("=" * 60)
    inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text'], input[type='number'], input[type='search'], input:not([type])")
    for inp in inputs:
        iid = inp.get_attribute("id")
        iname = inp.get_attribute("name")
        iplaceholder = inp.get_attribute("placeholder")
        iclass = inp.get_attribute("class")
        print(f"  ID: {iid}  Name: {iname}  Placeholder: {iplaceholder}  Class: {iclass}")
    print()

    # ─── Find ALL buttons ────────────────────────────────────────────
    print("=" * 60)
    print("BUTTONS:")
    print("=" * 60)
    buttons = driver.find_elements(By.CSS_SELECTOR, "input[type='submit'], input[type='button'], button")
    for btn in buttons:
        bid = btn.get_attribute("id")
        bclass = btn.get_attribute("class")
        btype = btn.get_attribute("type")
        btext = btn.text or btn.get_attribute("value") or ""
        print(f"  ID: {bid}  Type: {btype}  Class: {bclass}  Text: '{btext}'")
    print()

    # ─── Check for iframes ──────────────────────────────────────────
    print("=" * 60)
    print("IFRAMES:")
    print("=" * 60)
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    if iframes:
        for i, iframe in enumerate(iframes):
            src = iframe.get_attribute("src")
            iid = iframe.get_attribute("id")
            print(f"  [{i}] ID: {iid}  SRC: {src}")
    else:
        print("  None found")
    print()

    # ─── Check if this is an Angular/React app ──────────────────────
    print("=" * 60)
    print("FRAMEWORK DETECTION:")
    print("=" * 60)
    page_source = driver.page_source
    if "ng-" in page_source or "angular" in page_source.lower() or "_ngcontent" in page_source:
        print("  Detected: Angular")
    if "react" in page_source.lower() or "__react" in page_source or "data-reactroot" in page_source:
        print("  Detected: React")
    if "vue" in page_source.lower() or "data-v-" in page_source:
        print("  Detected: Vue.js")
    if "asp.net" in page_source.lower() or "__VIEWSTATE" in page_source:
        print("  Detected: ASP.NET")
    if "jquery" in page_source.lower():
        print("  Detected: jQuery")
    print()

    print("=" * 60)
    print(f"CURRENT URL: {driver.current_url}")
    print(f"PAGE TITLE:  {driver.title}")
    print("=" * 60)

    print("\nDone! Press Enter to close browser...")
    input()

except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
    try:
        driver.save_screenshot("debug_error_screenshot.png")
        with open("debug_page_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("\nSaved debug_error_screenshot.png and debug_page_source.html")
    except:
        pass

finally:
    driver.quit()
    print("Browser closed.")