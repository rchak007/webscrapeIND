#!/usr/bin/env python3
"""Debug: Print exact village dropdown options after selecting district + mandal."""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import time

opts = Options()
opts.page_load_strategy = "none"
d = webdriver.Chrome(options=opts)
d.get("https://registration.ap.gov.in/igrs/ppProperty")

# Wait for district dropdown to have options
print("Waiting for page to load...")
for i in range(30):
    try:
        s = Select(d.find_element(By.NAME, "district"))
        real = [o.text for o in s.options if "SELECT" not in o.text.upper()]
        if len(real) > 0:
            print(f"  Page ready after {(i+1)*2}s ({len(real)} districts)")
            break
    except:
        pass
    time.sleep(2)

# Print all district options
print("\n--- DISTRICT OPTIONS ---")
s = Select(d.find_element(By.NAME, "district"))
for o in s.options:
    print(f"  {repr(o.text)}")

# Select district with workaround
print("\nSelecting district: KAKINADA (with workaround)...")
s = Select(d.find_element(By.NAME, "district"))
# Pick something else first
first_real = [o.text for o in s.options if "SELECT" not in o.text.upper()][0]
s.select_by_visible_text(first_real)
print(f"  Temp selected: {first_real}")
time.sleep(2)

s = Select(d.find_element(By.NAME, "district"))
s.select_by_visible_text("KAKINADA")
print("  Selected: KAKINADA")
time.sleep(4)

# Wait for mandal to populate
print("\nWaiting for mandal dropdown...")
for i in range(15):
    try:
        s = Select(d.find_element(By.NAME, "Mandal"))
        real = [o.text for o in s.options if "SELECT" not in o.text.upper()]
        if len(real) > 0:
            print(f"  Mandal populated ({len(real)} options)")
            break
    except:
        pass
    time.sleep(2)

# Print all mandal options
print("\n--- MANDAL OPTIONS ---")
s = Select(d.find_element(By.NAME, "Mandal"))
for o in s.options:
    print(f"  {repr(o.text)}")

# Select mandal with workaround
print("\nSelecting mandal: KAKINADA (URBAN)...")
s = Select(d.find_element(By.NAME, "Mandal"))
first_real = [o.text for o in s.options if "SELECT" not in o.text.upper()][0]
s.select_by_visible_text(first_real)
print(f"  Temp selected: {first_real}")
time.sleep(2)

s = Select(d.find_element(By.NAME, "Mandal"))
s.select_by_visible_text("KAKINADA (URBAN)")
print("  Selected: KAKINADA (URBAN)")
time.sleep(4)

# Wait for village to populate
print("\nWaiting for village dropdown...")
for i in range(15):
    try:
        s = Select(d.find_element(By.NAME, "Village"))
        real = [o.text for o in s.options if "SELECT" not in o.text.upper()]
        if len(real) > 0:
            print(f"  Village populated ({len(real)} options)")
            break
    except:
        pass
    time.sleep(2)

# Print ALL village options with repr()
print("\n--- VILLAGE OPTIONS (exact text) ---")
s = Select(d.find_element(By.NAME, "Village"))
for o in s.options:
    val = o.get_attribute("value")
    print(f"  text={repr(o.text)}  value={repr(val)}")

print("\nDone! Press Enter to close browser...")
input()
d.quit()