from lxml import html
from datetime import datetime
import httpx
import pika
import json
import traceback

main_urls = {"hepsiburada_tel": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=2147483642_371965&tab=allproducts",
             "hepsiburada_pc": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=2147483646_3000500&tab=allproducts",
             "hepsiburada_ev_elektronigi": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=2147483638&tab=allproducts",
             "hepsiburada_oyun_oyun_konsolu": "https://www.hepsiburada.com/magaza/hepsiburada?kategori=60003054_2147483602&tab=allproducts",
             "kolaysepet": "https://www.hepsiburada.com/magaza/kolaysepet?kategori=2147483638&tab=allproducts"}

one_page_product_count = 36

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

print("----------------------------------------------------------------")
print(datetime.now()) 
for name, main_url in main_urls.items():
    try:
        print("name", name)
        page = 0
        links = []
        prices = []
        total_product_count = 0
        while True:
            page += 1
            if page == 1:
                page_link = ""
                response_content = get_response_from_url(url=main_url+page_link)
                tree = html.fromstring(response_content)
                total_product_count = tree.xpath('//span[contains(@class, "totalProductCount")]/text()')[0]
                total_product_count = int(total_product_count)
            else:
                page_link = f"&sayfa={page}"
                response_content = get_response_from_url(url=main_url+page_link)
                tree = html.fromstring(response_content)

            price_divs = tree.xpath("//div[@data-test-id='price-current-price']/text()")
            if price_divs:
                prices.extend([price.strip() for price in price_divs])
                prices = [
                    float(item.replace(".", "").replace(",", ".")) if isinstance(item, str) else item 
                    for item in prices
                ]

            product_cards = tree.xpath("//li[starts-with(@class, 'productListContent')]")
            for product in product_cards:
                link_divs = product.xpath(".//a[@title]/@href")
                if link_divs:
                    links.extend([
                        "https://www.hepsiburada.com" + link.strip() for link in link_divs if link.strip()
                    ])

            if total_product_count / 36 < page:
                break
        link_price = dict(zip(links, prices))
        data = {
            "time": datetime.now().isoformat(),
            "name": name,
            "link_price": link_price}
        
        message_json = json.dumps(data)
        #print(link_price)
        publish_message(message_json)
    except:
        print(traceback.format_exc())
        continue
    