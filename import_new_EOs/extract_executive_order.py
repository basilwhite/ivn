import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def fetch_executive_order(order_number):
    url = f"https://www.federalregister.gov/executive-order/{order_number}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    print(f"Fetched URL: {url} (status code: {response.status_code})")
    if response.status_code != 200:
        print(f"Failed to fetch page: {url}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    content_div = soup.find('div', id='fulltext_content_area')
    if not content_div:
        print("Content area not found. Saving HTML to 'debug_output.html' for inspection.")
        with open('debug_output.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(response.text[:500])
        return
    header = content_div.find('h1')
    title = header.get_text(strip=True) if header else "No header found."
    paragraphs = content_div.find_all('p')
    summary = "\n".join([p.get_text(strip=True) for p in paragraphs])
    return {
        'Executive Order #': order_number,
        'URL': url,
        'Title': title,
        'Summary': summary
    }

def fetch_range_of_orders(oldest, newest):
    results = []
    total = newest - oldest + 1
    start_time = time.time()
    for idx, order_number in enumerate(range(oldest, newest + 1), 1):
        print(f"Processing Executive Order {order_number} ({idx}/{total})...")
        iter_start = time.time()
        result = fetch_executive_order(str(order_number))
        iter_end = time.time()
        if result:
            results.append(result)
        # Estimate time remaining
        elapsed = iter_end - start_time
        avg_time = elapsed / idx
        remaining = total - idx
        est_remaining = avg_time * remaining
        print(f"Estimated time remaining: {est_remaining:.1f} seconds")
    if results:
        df = pd.DataFrame(results)
        df.to_excel('executive_order_output.xlsx', index=False)
        print("Output written to executive_order_output.xlsx")
    else:
        print("No valid executive orders found in the specified range.")

if __name__ == "__main__":
    oldest = int(input("Enter the oldest Executive Order number: "))
    newest = int(input("Enter the newest Executive Order number: "))
    fetch_range_of_orders(oldest, newest)
