import csv
import time
import urllib.parse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

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
    return list(set(skus))

def get_product_details(driver, sku):
    url = f"https://www.atnetworks.com/Products/overview/{sku}"
    driver.get(url)
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#body-main > div.product-view > div:nth-child(1) > div:nth-child(1) > div"))
        )
    except Exception:
        return None
    product_name = ""
    manufacturer = ""
    part_number = ""
    upc = ""
    unspsc = ""
    main_image = ""
    description = ""
    specifications = ""
    try:
        product_name = driver.find_element(By.CSS_SELECTOR, "#body-main > div.product-view > div:nth-child(1) > div:nth-child(1) > div").text.strip()
    except Exception:
        pass
    try:
        manufacturer = driver.find_element(By.CSS_SELECTOR, "div#mfr.readonly-text").text.strip()
    except Exception:
        pass
    try:
        part_number = driver.find_element(By.CSS_SELECTOR, "div#partnum.readonly-text").text.strip()
    except Exception:
        pass
    try:
        unspsc = driver.find_element(By.CSS_SELECTOR, "#unspsc").text.strip()
    except Exception:
        pass
    try:
        upc = driver.find_element(By.CSS_SELECTOR, "#upc").text.strip()
    except Exception:
        pass
    try:
        style_attr = driver.find_element(By.CSS_SELECTOR, "div#product-first-img.product-img").get_attribute("style")
        if style_attr:
            start = style_attr.find("url(")
            if start != -1:
                start += 4
                end = style_attr.find(")", start)
                if end != -1:
                    main_image = style_attr[start:end].replace("'", "").replace('"', "").strip()
                    if main_image.startswith("//"):
                        main_image = "https:" + main_image
                    if main_image == "https://static.channelonline.com/STATICuLYssXMSdO/resources/staticj/img/nopic.jpg":
                        main_image = ""
    except Exception:
        pass
    try:
        paragraphs = driver.find_element(By.CSS_SELECTOR, "div.ccs-ds-textMkt").find_elements(By.TAG_NAME, "p")
        desc_paragraph = "\n\n".join(p.text.strip() for p in paragraphs)
    except Exception:
        desc_paragraph = ""
    try:
        bullet_html = driver.find_element(By.CSS_SELECTOR, "div.ccs-ds-textKsp").get_attribute("innerHTML")
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
        else:
            description += bullet_text
    try:
        details_html = driver.find_element(By.CSS_SELECTOR, "div#tab-specs").get_attribute("outerHTML")
        soup = BeautifulSoup(details_html, "html.parser")
        specs_table = soup.select_one("div#product-specs > table.costandard")
        if specs_table:
            tbodies = specs_table.find_all("tbody")
            for tbody in tbodies:
                th = tbody.find("th")
                section = th.get_text(strip=True) if th else "General"
                rows = tbody.find_all("tr")
                section_text = ""
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        section_text += f"{label}: {value}\n"
                if section_text:
                    specifications += f"{section}:\n{section_text}\n"
    except Exception:
        pass
    try:
        li_elements = driver.find_element(By.CSS_SELECTOR, "#body-main > ol").find_elements(By.TAG_NAME, "li")
        category_list = [li.text.strip() for li in li_elements if li.text.strip() and "Back to Results" not in li.text]
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

def process_sku(sku):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    try:
        return get_product_details(driver, sku)
    except:
        return None
    finally:
        driver.quit()

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
                    WebDriverWait(driver, 10).until(
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
                    with ThreadPoolExecutor(max_workers=8) as executor:
                        futures = {executor.submit(process_sku, sku): sku for sku in skus}
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
                    try:
                        next_page = page + 1
                        next_button = driver.find_element(By.XPATH, f"//a[contains(@href, 'navigateToPage({next_page})')]")
                        old_product = driver.find_element(By.CSS_SELECTOR, "a[href*='/Products/overview/']")
                        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                        driver.execute_script("arguments[0].click();", next_button)
                        WebDriverWait(driver, 10).until(EC.staleness_of(old_product))
                        time.sleep(1)
                        page += 1
                    except:
                        break
            finally:
                driver.quit()
    print(f"\nAll data saved to {output_file}")

if __name__ == "__main__":
    main()
