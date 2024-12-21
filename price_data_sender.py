from lxml import html
from datetime import datetime
import httpx
import pika
import json
import traceback
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import randint
from urllib.parse import urlparse, urlencode, urlunparse, parse_qs

def publish_message(message):
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()
    channel.queue_declare(queue='price_queue')
    channel.basic_publish(exchange='', routing_key='price_queue', body=message)
    connection.close()

def get_response_from_url(url):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }
    cookies = {
        "gcl_au": "1.1.581871697.1732298393",
        "useinternal": "true",
        "_gid": "GA1.2.2141075332.1732298393",
    }
    with httpx.Client(headers=headers, cookies=cookies) as client:
        try:
            response = client.get(url)
            response.raise_for_status()
            return response.content
        except httpx.ReadTimeout:
            print(f"Timeout error while fetching URL: {url}")
            return None
        except httpx.RequestError as e:
            print(f"Request error for URL {url}: {e}")
            return None
    
def avoid_cache(main_url):
    random_number = randint(1000000, 9999999)

    parsed_url = urlparse(main_url)
    query_params = parse_qs(parsed_url.query)

    query_params['filtreler'] = [f"fiyat:min-{random_number}"]
    new_query = urlencode(query_params, doseq=True)

    updated_url = urlunparse(parsed_url._replace(query=new_query))
    return updated_url

def process_url(name, main_url):
    try:
        link_price = {}
        total_product_count = 0

        # İlk sayfa talebini al ve toplam ürün sayısını öğren
        initial_url = avoid_cache(main_url)
        response_content = get_response_from_url(url=initial_url)
        if response_content is None:
            return {name: None}
        tree = html.fromstring(response_content)
        total_product_count = tree.xpath('//span[contains(@class, "totalProductCount")]/text()')[0]
        total_product_count = int(total_product_count)

        # Toplam sayfa sayısını hesapla
        total_pages = (total_product_count + 35) // 36  # 36 ürün bir sayfada

        def process_page(page):
            page_link = "" if page == 1 else f"&sayfa={page}"
            non_cached_url = avoid_cache(main_url)
            response_content = get_response_from_url(url=non_cached_url + page_link)
            if response_content is None:
                return {}
            tree = html.fromstring(response_content)

            page_link_price = {}
            product_cards = tree.xpath("//li[starts-with(@class, 'productListContent')]")
            for product in product_cards:
                campaign = product.xpath(".//div[@data-test-id='campaign']/text()")
                if campaign:
                    text = campaign[0]
                    pattern = r"\b\d{1,3}(?:\.\d{3})*,\d{2}\b"
                    price = re.findall(pattern, text)
                else:
                    price = product.xpath(".//div[@data-test-id='price-current-price']/text()")

                if not price:
                    continue
                price = float(price[0].replace(".", "").replace(",", "."))

                link = product.xpath(".//a[@title]/@href")
                if not link:
                    continue
                link = "https://www.hepsiburada.com" + link[0].strip()

                if price and link:
                    page_link_price[link] = price

            return page_link_price

        # Sayfaları paralel işleme
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_page, page) for page in range(1, total_pages + 1)]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        link_price.update(result)
                except Exception as e:
                    print(f"Error processing future: {e}")

        data = {
            "time": datetime.now().isoformat(),
            "name": name,
            "link_price": link_price
        }

        message_json = json.dumps(data)
        #print(link_price)
        publish_message(message_json)
        return {name: len(link_price)}
    except:
        print(f"Error in {name}: {traceback.format_exc()}")
        return {name: None}

def main():
    main_urls = {"hepsiburada_tel": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=2147483642_371965&tab=allproducts",
             "hepsiburada_pc": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=2147483646_3000500&tab=allproducts",
             "hepsiburada_ev_elektronigi": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=2147483638&tab=allproducts",
             "hepsiburada_oyun_oyun_konsolu": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=60003054_2147483602&tab=allproducts",
             "kolaysepet": "https://www.hepsiburada.com/magaza/kolaysepet?kategori=2147483638&tab=allproducts"}
   
    print("----------------------------------------------------------------")
    t1 = datetime.now()
    print("Start Time:", t1)

    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {
            executor.submit(process_url, name, url): name for name, url in main_urls.items()
        }

        for future in as_completed(future_to_url):
            name = future_to_url[future]
            try:
                result = future.result()
                if result:
                    results.update(result)
            except Exception as e:
                print(f"Error in processing {name}: {e}")

    t2 = datetime.now()
    print("End Time:", t2)
    print("Spend Time:", t2 - t1)
    print("Final Results:", results)

if __name__ == "__main__":
    main()