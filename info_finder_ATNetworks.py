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

# Helper function to scrape product SKUs from the current page.
def scrape_current_page(driver):
    skus = []
    try:
        # Wait until product links are present.
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='/Products/overview/']"))
        )
        product_elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/Products/overview/']")
        print(f"Found {len(product_elements) // 2} product links on current page.")
        for elem in product_elements:
            # Only count the element if it is visible.
            if elem.is_displayed():
                href = elem.get_attribute("href")
                if "/Products/overview/" in href:
                    sku = href.split("/")[-1]
                    skus.append(sku)
    except Exception as e:
        print("Error scraping current page:", e)
    # Remove duplicates.
    unique_skus = list(set(skus))
    return unique_skus

# Function to retrieve product details given a SKU.
def get_product_details(driver, sku):
    url = f"https://www.atnetworks.com/Products/overview/{sku}"
    driver.get(url)
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "#body-main > div.product-view > div:nth-child(1) > div:nth-child(1) > div"
            ))
        )
    except Exception:
        return None  # Product not found

    product_name = ""
    manufacturer = ""
    part_number = ""
    description = ""
    specifications = ""
    main_image = ""

    try:
        product_name_elem = driver.find_element(By.CSS_SELECTOR,
            "#body-main > div.product-view > div:nth-child(1) > div:nth-child(1) > div")
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
    except Exception:
        pass

    try:
        description_elem = driver.find_element(By.CSS_SELECTOR, "div.ccs-ds-textMkt")
        description = description_elem.text.strip()
    except Exception:
        pass

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

    return {
        "SKU": sku,
        "Product Name": product_name,
        "Manufacturer": manufacturer,
        "Part Number": part_number,
        "Main Image": main_image,
        "Description": description,
        "Specifications": specifications
    }

# Function to process a single SKU using a separate driver instance.
def process_sku(sku):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=chrome_options)
    try:
        data = get_product_details(driver, sku)
    except Exception as e:
        print(f"Error processing SKU {sku}: {e}")
        data = None
    finally:
        driver.quit()
    return data

# Main function: Process each category, scrape pages one by one,
# process the products on each page concurrently, add category information, and append to CSV.
def main():
    category_file = "categories_ATNetworks.txt"  # File with category URLs (one per line)
    output_file = "products_ATNetworks.csv"
    
    # Define CSV fields (added Category after Product Name).
    base_fields = ["SKU", "Product Name", "Category", "Manufacturer", "Part Number", "Main Image", "Description", "Specifications"]
    
    # Open CSV for writing.
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=base_fields, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        # Read category URLs.
        with open(category_file, "r", encoding="utf-8") as f:
            category_urls = [line.strip() for line in f if line.strip()]
        
        # Process each category.
        for category_url in category_urls:
            # Extract category name from the URL using the "cn1" query parameter.
            parsed_url = urllib.parse.urlparse(category_url)
            params = urllib.parse.parse_qs(parsed_url.query)
            category_name = params.get("cn1", [None])[0]
            if not category_name:
                category_name = category_url  # Fallback if parameter not found.
                
            print(f"\nProcessing category: {category_name}")
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            driver = webdriver.Chrome(options=chrome_options)
            try:
                driver.get(category_url)
                
                # Force the page to display 50 results.
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#number-results-50"))
                    )
                    driver.execute_script("document.querySelector('#number-results-50').click();")
                    print("Set results per page to 50.")
                    time.sleep(2)  # Allow time for the page to refresh.
                except Exception as e:
                    print("Could not set results per page:", e)
                
                page = 1
                while True:
                    print(f"\nScraping page {page} of {category_name}")
                    skus = scrape_current_page(driver)
                    if not skus:
                        print("No products found on this page. Ending pagination for this category.")
                        break
                    
                    # Process the SKUs concurrently.
                    results_page = []
                    with ThreadPoolExecutor(max_workers=9) as executor:
                        futures = {executor.submit(process_sku, sku): sku for sku in skus}
                        for future in as_completed(futures):
                            sku = futures[future]
                            try:
                                product_data = future.result()
                                if product_data is not None:
                                    # Add category name info to the product record.
                                    product_data["Category"] = category_name
                                    results_page.append(product_data)
                                    print(f"Data for SKU {sku} saved.")
                                else:
                                    print(f"SKU {sku} skipped.")
                            except Exception as e:
                                print(f"Error processing SKU {sku}: {e}")
                    
                    # Write this page’s results to CSV.
                    for product in results_page:
                        writer.writerow(product)
                    csvfile.flush()
                    print(f"Page {page}: Written {len(results_page)} records to CSV.")
                    
                    # Attempt to navigate to the next page.
                    try:
                        next_page = page + 1
                        next_button = driver.find_element(By.XPATH, f"//a[contains(@href, 'navigateToPage({next_page})')]")
                        print(f"Navigating to page {next_page}")
                        # Capture a reference to a visible product element on the current page.
                        old_product = driver.find_element(By.CSS_SELECTOR, "a[href*='/Products/overview/']")
                        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                        driver.execute_script("arguments[0].click();", next_button)
                        WebDriverWait(driver, 10).until(EC.staleness_of(old_product))
                        time.sleep(1)
                        page += 1
                    except Exception as e:
                        print("No further pages found or error navigating to next page:", e)
                        break
            finally:
                driver.quit()
    print(f"\nAll data saved to {output_file}")

if __name__ == "__main__":
    main()
