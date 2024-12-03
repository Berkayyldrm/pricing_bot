import requests
from lxml import html
from datetime import datetime
import httpx
import pika
import json

main_urls = {"hepsiburada_tel": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=2147483642_371965&tab=allproducts",
             "kolaysepet": "https://www.hepsiburada.com/magaza/kolaysepet?kategori=2147483638&tab=allproducts"}

one_page_product_count = 36

def publish_message(message):
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
    channel = connection.channel()

    channel.queue_declare(queue='test_queue')

    channel.basic_publish(exchange='', routing_key='test_queue', body=message)

    connection.close()

def get_response_from_url(url):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "max-age=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "If-Modified-Since": "Fri, 22 Nov 2024 18:10:33 GMT",
    }
    cookies = {
        "gcl_au": "1.1.581871697.1732298393",
        "useinternal": "true",
        "_gid": "GA1.2.2141075332.1732298393",
    }
    with httpx.Client(headers=headers, cookies=cookies) as client:
        response = client.get(url)
        return response.content
    
for name, main_url in main_urls.items():
    print("name", name)
    page = 0
    links = []
    prices = []
    while True:
        page += 1
        print("page", page)
        if page == 1:
            page_link = ""
        else:
            page_link = f"&sayfa={page}"
        response_content = get_response_from_url(url=main_url+page_link)

        tree = html.fromstring(response_content)

        # Tüm 'data-test-id="price-current-price"' div'lerini bul ve metinlerini al
        price_divs = tree.xpath("//div[@data-test-id='price-current-price']/text()")
        if price_divs:
            prices.extend([price.strip() for price in price_divs])
            prices = [
                float(item.replace(".", "").replace(",", ".")) if isinstance(item, str) else item 
                for item in prices
            ]
        else:
            print("Fiyat bilgisi bulunamadı.")

        total_product_count_xpath = f"/html/body/div[2]/div/div/main/div[1]/div/div/div[2]/div/div[2]/div[2]/div/div/div[1]/div/div[2]/div/div/div/div/div/div/div/div[1]/div/div[1]/span"
        total_product_count = int(tree.xpath(total_product_count_xpath)[0].text)
        for i in range(one_page_product_count):
            i += 1
            general_product_link_xpath = f"/html/body/div[2]/div/div/main/div/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div[4]/div/div[2]/div/div/div/div/div/div/ul/li[{i}]/div/a"
            general_product_link = tree.xpath(f"{general_product_link_xpath}/@href")

            if general_product_link:
                l = general_product_link[0].text if hasattr(general_product_link[0], 'text') else str(general_product_link[0])
                links.append(l)
        if total_product_count / 36 < page:
            print("Last page executed.")
            break
    link_price = dict(zip(links, prices))
    data = {
        "time": datetime.now().isoformat(),
        "name": name,
        "link_price": link_price}
    
    message_json = json.dumps(data)
    publish_message(message_json)
    