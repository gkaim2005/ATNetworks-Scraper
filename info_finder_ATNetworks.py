import csv
import time
import urllib.parse
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import queue

# Global WebDriver pool and driver list for cleanup
driver_pool = queue.Queue()
drivers = []

def create_driver_pool(pool_size=5):
    """Initialize a pool of WebDriver instances."""
    for _ in range(pool_size):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        drivers.append(driver)
        driver_pool.put(driver)

def scrape_current_page(driver):
    """Extract SKUs from the current category page."""
    skus = []
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='/Products/overview/']"))
        )
        product_elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/Products/overview/']")
        print(f"Found {len(product_elements) // 2} product links on current page.")
        for elem in product_elements:
            if elem.is_displayed():
                href = elem.get_attribute("href")
                if "/Products/overview/" in href:
                    sku = href.split("/")[-1]
                    skus.append(sku)
    except Exception as e:
        print("Error scraping current page:", e)
    return list(set(skus))

def get_product_details(sku):
    """Fetch product details using a WebDriver from the pool."""
    driver = driver_pool.get()
    try:
        url = f"https://www.atnetworks.com/Products/overview/{sku}"
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#body-main"))
        )
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Product Name
        product_name = ""
        try:
            product_name_elem = driver.find_element(By.CSS_SELECTOR, "#body-main > div.product-view > div:nth-child(1) > div:nth-child(1) > div")
            product_name = product_name_elem.text.strip()
        except:
            pass

        # Manufacturer
        manufacturer = ""
        try:
            manufacturer_elem = driver.find_element(By.CSS_SELECTOR, "div#mfr.readonly-text")
            manufacturer = manufacturer_elem.text.strip()
        except:
            pass

        # Part Number
        part_number = ""
        try:
            part_elem = driver.find_element(By.CSS_SELECTOR, "div#partnum.readonly-text")
            part_number = part_elem.text.strip()
        except:
            pass

        # UNSPSC Code
        unspsc = ""
        try:
            unspsc_elem = driver.find_element(By.CSS_SELECTOR, "#unspsc")
            unspsc = unspsc_elem.text.strip()
        except:
            pass

        # UPC
        upc = ""
        try:
            upc_elem = driver.find_element(By.CSS_SELECTOR, "#upc")
            upc = upc_elem.text.strip()
        except:
            pass

        # Main Image
        main_image = ""
        try:
            img_elem = driver.find_element(By.CSS_SELECTOR, "div#product-first-img.product-img")
            style = img_elem.get_attribute("style")
            start = style.find("url(")
            if start != -1:
                start += 4
                end = style.find(")", start)
                if end != -1:
                    main_image = style[start:end].replace("'", "").replace('"', "").strip()
                    if main_image.startswith("//"):
                        main_image = "https:" + main_image
                    if main_image == "https://static.channelonline.com/STATICuLYssXMSdO/resources/staticj/img/nopic.jpg":
                        main_image = ""
        except:
            pass

        # Description
        desc_paragraph = ""
        try:
            paras = driver.find_element(By.CSS_SELECTOR, "div.ccs-ds-textMkt").find_elements(By.TAG_NAME, "p")
            desc_paragraph = "\n\n".join(p.text.strip() for p in paras)
        except:
            pass

        bullet_text = ""
        try:
            bullet_elem = driver.find_element(By.CSS_SELECTOR, "div.ccs-ds-textKsp")
            items = bullet_elem.find_elements(By.TAG_NAME, "li")
            bullet_text = "\n".join(f"- {li.text.strip()}" for li in items)
        except:
            pass

        description = ""
        if desc_paragraph or bullet_text:
            description = "Product Description:\n"
            if desc_paragraph:
                description += desc_paragraph
                if bullet_text:
                    description += "\n\n" + bullet_text
            else:
                description += bullet_text

        # Specifications using BeautifulSoup as requested
        specifications = ""
        try:
            specs_table = soup.select_one("div#product-specs > table.costandard")
            if specs_table:
                tbodies = specs_table.find_all("tbody")
                current_section = "General"
                for tbody in tbodies:
                    th = tbody.find("th")
                    if th:
                        current_section = th.get_text(strip=True)
                    section_text = ""
                    rows = tbody.find_all("tr")
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) == 2:
                            label = cells[0].get_text(strip=True)
                            value = cells[1].get_text(strip=True)
                            section_text += f"{label}: {value}\n"
                    if section_text:
                        specifications += f"{current_section}:\n{section_text}\n"
        except:
            pass

        # Category
        full_category = ""
        try:
            li_elements = driver.find_elements(By.CSS_SELECTOR, "#body-main > ol > li")
            category_list = [li.text.strip() for li in li_elements if "Back to Results" not in li.text]
            full_category = " / ".join(category_list)
        except:
            pass

        return {
            "SKU": sku,
            "Product Name": product_name,
            "Category": full_category,
            "Manufacturer": manufacturer,
            "Part #": part_number,
            "UNSPSC Code": unspsc,
            "UPC": upc,
            "Main Image": main_image,
            "Description": description,
            "Specifications": specifications
        }
    except Exception as e:
        print(f"Error fetching details for SKU {sku}: {e}")
        return None
    finally:
        driver_pool.put(driver)  # Return WebDriver to pool

def main():
    category_file = "categories_ATNetworks.txt"
    output_file = "products_ATNetworks.csv"
    base_fields = [
        "SKU",
        "Product Name",
        "Category",
        "Manufacturer",
        "Part #",
        "UNSPSC Code",
        "UPC",
        "Main Image",
        "Description",
        "Specifications"
    ]

    # Create WebDriver pool
    create_driver_pool(pool_size=5)

    try:
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=base_fields, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            with open(category_file, "r", encoding="utf-8") as f:
                category_urls = [line.strip() for line in f if line.strip()]
            for category_url in category_urls:
                parsed_url = urllib.parse.urlparse(category_url)
                params = urllib.parse.parse_qs(parsed_url.query)
                category_name = params.get("cn1", [category_url])[0]
                print(f"\nProcessing category: {category_name}")
                chrome_options = Options()
                chrome_options.add_argument("--headless=new")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
                try:
                    driver.get(category_url)
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#number-results-50"))
                        )
                        driver.execute_script("document.querySelector('#number-results-50').click();")
                        time.sleep(2)
                    except:
                        pass
                    page = 1
                    while True:
                        print(f"\nScraping page {page} of {category_name}")
                        skus = scrape_current_page(driver)
                        if not skus:
                            break
                        results_page = []
                        with ThreadPoolExecutor(max_workers=25) as executor:
                            futures = {executor.submit(get_product_details, sku): sku for sku in skus}
                            for future in as_completed(futures):
                                sku = futures[future]
                                data = future.result()
                                if data:
                                    results_page.append(data)
                                    print(f"Data for SKU {sku} saved.")
                                else:
                                    print(f"SKU {sku} skipped.")
                        for product in results_page:
                            writer.writerow(product)
                        csvfile.flush()
                        print(f"Page {page}: Written {len(results_page)} records to CSV.")
                        next_page = page + 1
                        success = False
                        for attempt in range(1, 4):
                            try:
                                next_button = driver.find_element(
                                    By.XPATH,
                                    f"//a[contains(@href, 'navigateToPage({next_page})')]"
                                )
                                old_product = driver.find_element(
                                    By.CSS_SELECTOR,
                                    "a[href*='/Products/overview/']"
                                )
                                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                                driver.execute_script("arguments[0].click();", next_button)
                                wait_time = 10 * attempt
                                WebDriverWait(driver, wait_time).until(EC.staleness_of(old_product))
                                time.sleep(1)
                                page += 1
                                success = True
                                break
                            except Exception as e:
                                print(f"Attempt {attempt} to load page {next_page} failed: {e}")
                                if attempt < 3:
                                    time.sleep(5 * attempt)
                                else:
                                    print(f"No next page after {attempt} attempts, stopping pagination.")
                        if not success:
                            break
                finally:
                    driver.quit()
    finally:
        # Cleanup WebDriver pool
        for driver in drivers:
            driver.quit()
    print(f"\nAll data saved to {output_file}")

if __name__ == "__main__":
    main()
