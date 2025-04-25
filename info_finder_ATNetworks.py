import csv
import time
import urllib.parse
import tempfile
import random
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scrape_current_page(driver):
    skus = []
    try:
        WebDriverWait(driver, 15).until(
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
    unique_skus = list(set(skus))
    return unique_skus

def get_product_details(driver, sku, retries=3, delay=5):
    url = f"https://www.atnetworks.com/Products/overview/{sku}"
    for attempt in range(retries):
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#body-main > div.product-view > div:nth-child(1) > div:nth-child(1) > div"))
            )
            # Scraping code goes here...
            product_name = ""
            manufacturer = ""
            part_number = ""
            upc = ""
            unspsc = ""
            main_image = ""
            description = ""
            specifications = ""
            try:
                product_name_elem = driver.find_element(By.CSS_SELECTOR, "#body-main > div.product-view > div:nth-child(1) > div:nth-child(1) > div")
                product_name = product_name_elem.text.strip()
            except Exception:
                pass
            try:
                manufacturer_elem = driver.find_element(By.CSS_SELECTOR, "div#mfr.readonly-text")
                manufacturer = manufacturer_elem.text.strip()
            except Exception:
                pass
            try:
                part_number_elem = driver.find_element(By.CSS_SELECTOR, "div#partnum.readonly-text")
                part_number = part_number_elem.text.strip()
            except Exception:
                pass
            try:
                unspsc_elem = driver.find_element(By.CSS_SELECTOR, "#unspsc")
                unspsc = unspsc_elem.text.strip()
            except Exception:
                pass
            try:
                upc_elem = driver.find_element(By.CSS_SELECTOR, "#upc")
                upc = upc_elem.text.strip()
            except Exception:
                pass
            try:
                image_elem = driver.find_element(By.CSS_SELECTOR, "div#product-first-img.product-img")
                style_attr = image_elem.get_attribute("style")
                if style_attr:
                    start_index = style_attr.find("url(")
                    if start_index != -1:
                        start_index += len("url(")
                        end_index = style_attr.find(")", start_index)
                        if end_index != -1:
                            main_image = style_attr[start_index:end_index].replace("'", "").replace('"', "").strip()
                            if main_image.startswith("//"):
                                main_image = "https:" + main_image
                            if main_image == "https://static.channelonline.com/STATICuLYssXMSdO/resources/staticj/img/nopic.jpg":
                                main_image = ""
            except Exception:
                pass

            try:
                desc_mkt_elem = driver.find_element(By.CSS_SELECTOR, "div.ccs-ds-textMkt")
                paragraphs = desc_mkt_elem.find_elements(By.TAG_NAME, "p")
                desc_paragraph = "\n\n".join(p.text.strip() for p in paragraphs)
            except Exception:
                desc_paragraph = ""

            try:
                bullets_elem = driver.find_element(By.CSS_SELECTOR, "div.ccs-ds-textKsp")
                bullet_html = bullets_elem.get_attribute("innerHTML")
                bullet_soup = BeautifulSoup(bullet_html, "html.parser")
                bullet_list = [li.get_text(" ", strip=True) for li in bullet_soup.find_all("li")]
                bullet_text = "\n".join(f"- {item}" for item in bullet_list)
            except Exception:
                bullet_text = ""

            if desc_paragraph or bullet_text:
                description = "Product Description: \n"
                if desc_paragraph:
                    description += desc_paragraph
                    if bullet_text:
                        description += "\n\n" + bullet_text
                elif bullet_text:
                    description += bullet_text

            try:
                details_elem = driver.find_element(By.CSS_SELECTOR, "div#tab-specs")
                soup = BeautifulSoup(details_elem.get_attribute("outerHTML"), "html.parser")
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
            except Exception:
                pass

            try:
                breadcrumb_elem = driver.find_element(By.CSS_SELECTOR, "#body-main > ol")
                li_elements = breadcrumb_elem.find_elements(By.TAG_NAME, "li")
                category_list = []
                for li in li_elements:
                    txt = li.text.strip()
                    if "Back to Results" in txt:
                        continue
                    if txt:
                        category_list.append(txt)
                full_category = " / ".join(category_list)
            except Exception:
                full_category = ""

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
            print(f"Attempt {attempt+1} failed for SKU {sku}: {e}")
            time.sleep(delay)
    print(f"Skipping SKU {sku} after {retries} attempts.")
    return None

def process_sku(sku):
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/google-chrome"
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    try:
        data = get_product_details(driver, sku)
    except Exception as e:
        print(f"Error processing SKU {sku}: {e}")
        data = None
    finally:
        driver.quit()
    return data

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
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=base_fields, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        with open(category_file, "r", encoding="utf-8") as f:
            category_urls = [line.strip() for line in f if line.strip()]
        for category_url in category_urls:
            parsed_url = urllib.parse.urlparse(category_url)
            params = urllib.parse.parse_qs(parsed_url.query)
            category_name = params.get("cn1", [None])[0]
            if not category_name:
                category_name = category_url
            print(f"\nProcessing category: {category_name}")
            chrome_options = Options()
            chrome_options.binary_location = "/usr/bin/google-chrome"
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=chrome_options)
            try:
                driver.get(category_url)
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#number-results-50"))
                    )
                    driver.execute_script("document.querySelector('#number-results-50').click();")
                    print("Set results per page to 50.")
                    time.sleep(2)
                except Exception as e:
                    print("Could not set results per page:", e)
                page = 1
                while True:
                    print(f"\nScraping page {page} of {category_name}")
                    skus = scrape_current_page(driver)
                    if not skus:
                        print("No products found on this page. Ending pagination for this category.")
                        break
                    results_page = []
                    with ThreadPoolExecutor(max_workers=32) as executor:  # Increased to 32 workers
                        futures = {executor.submit(process_sku, sku): sku for sku in skus}
                        for future in as_completed(futures):
                            sku = futures[future]
                            try:
                                product_data = future.result()
                                if product_data is not None:
                                    results_page.append(product_data)
                                    print(f"Data for SKU {sku} saved.")
                                else:
                                    print(f"SKU {sku} skipped.")
                            except Exception as e:
                                print(f"Error processing SKU {sku}: {e}")
                    for product in results_page:
                        writer.writerow(product)
                    csvfile.flush()
                    print(f"Page {page}: Written {len(results_page)} records to CSV.")
                    try:
                        next_page = page + 1
                        driver.get(f"{category_url}&page={next_page}")
                        time.sleep(random.uniform(2.5, 4))
                    except Exception:
                        break
                    page += 1
            except Exception as e:
                print(f"Error processing category {category_name}: {e}")
            finally:
                driver.quit()

if __name__ == "__main__":
    main()
