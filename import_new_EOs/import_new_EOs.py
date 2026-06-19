import re
import time
import openpyxl
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# Create a new Excel workbook
wb = openpyxl.Workbook()
ws = wb.active
ws.title = "Executive Orders"
ws.append(["Executive Order Number", "First 300 Words"])


def get_first_300_words(text):
    words = re.findall(r'\b\w+\b', text)
    return ' '.join(words[:300])


def fetch_executive_orders(start_eo=14147, end_eo=14257):
    base_url = "https://www.federalregister.gov/executive-order/"


    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0')


    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


    for eo_num in range(start_eo, end_eo + 1):
        eo_url = f"{base_url}{eo_num}"
        print(f"Fetching EO {eo_num} from {eo_url}...")
        try:
            driver.get(eo_url)
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".article-body, main"))
            )
            try:
                article_body = driver.find_element(By.CLASS_NAME, "article-body")
            except:
                article_body = driver.find_element(By.TAG_NAME, "main")


            text = article_body.text
            first_300_words = get_first_300_words(text)
            ws.append([f"EO {eo_num}", first_300_words])
        except Exception as e:
            print(f"Error fetching EO {eo_num}: {e}")
            continue


        time.sleep(1)


    driver.quit()
    wb.save("executive_orders_300_words.xlsx")
    print("Done. File saved as executive_orders_300_words.xlsx")


# Run the script
fetch_executive_orders()




