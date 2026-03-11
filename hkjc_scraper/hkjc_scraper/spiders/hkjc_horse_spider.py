import scrapy
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import configparser
import re
import pandas as pd

class HKJC_Horse_Spider(scrapy.Spider):
    name = "hkjc_horse_spider"

    def __init__(self, *args, **kwargs):
        super(HKJC_Horse_Spider, self).__init__(*args, **kwargs)
        self.spider_type = "horse"

        ''' Init : Read the config '''
        config = configparser.ConfigParser()
        config.read('./scrapy.cfg')
        self.to_update_csv_path = config['support']['to_update_csv_path']
        self.horse_output_path = config['output']['horse_output_path']

        to_updateDF = pd.read_csv(self.to_update_csv_path)
        self.horse_urls = to_updateDF['馬名連結'].to_list()#[100:120]

        self.logger.info(f'The scraped data will be stored to path: {self.horse_output_path}')
        self.logger.info(f"{len(self.horse_urls)} urls to check")


    def start_requests(self):
        for horse_url in self.horse_urls:  # Fixed 'itmes' to 'items'
            yield scrapy.Request(
                url=horse_url,
                callback=self.check_horse
            )
    
    def check_horse(self, response):
        item = {
            '馬名': '未知',
            '馬匹編號': '未知',
            '出生地': '未知',
            '馬齡': '未知',
            '毛色': '未知',
            '性別': '未知',
            '進口類別': '未知',
            '今季獎金': '0',
            '總獎金': '0',
            '冠': '0',
            '亞': '0',
            '季': '0',
            '總出賽次數': '0',
            '最近十個賽馬日出賽場數': '0',
            '現在位置': '未知',
            '進口日期': '未知',
            '現時評分': '0',
            '季初評分': '0',
            '父系': '未知',
            '母系': '未知',
            '外祖父': '未知',
            '馬匹狀態': '未知'
        }

        title_element = response.css('.subsubheader .title_text::text').get()
        if title_element:
            # format usually like: "燭光晚餐 (J468)"
            match = re.search(r'(.+?)\s*\((.+?)\)(?:\s*\((.+?)\))?', title_element)
            if match:
                item['馬名'] = match.group(1).strip()
                item['馬匹編號'] = match.group(2).strip()
                # Check if there's a third group (second parentheses)
                if match.group(3):
                    item['馬匹狀態'] = match.group(3).strip()

        # Left table scraping
        left_table_rows = response.css('.table_top_right.table_eng_text tbody tr')

        for row in left_table_rows:
            cells = row.css('td::text').getall()
            if len(cells) >= 3:
                key = cells[0].strip() if cells[0] else ""
                value = cells[2].strip() if cells[2] else ""
                
                if "出生地 / 馬齡" in key:
                    parts = value.split('/')
                    if len(parts) >= 2:
                        item['出生地'] = parts[0].strip()
                        item['馬齡'] = parts[1].strip()
                    else:
                        item['出生地'] = value

                elif "毛色 / 性別" in key:
                    parts = value.split('/')
                    if len(parts) >= 2:
                        item['毛色'] = parts[0].strip()
                        item['性別'] = parts[1].strip()
                    else:
                        item['毛色'] = value
                elif "進口類別" in key:
                    item['進口類別'] = value
                elif "今季獎金" in key:
                    item['今季獎金'] = value
                elif "總獎金" in key:
                    item['總獎金'] = value
                elif "冠-亞-季-總出賽次數" in key:
                    first_place_cnt, second_place_cnt, third_place_cnt, total_race = value.split('-')
                    item['冠'] = first_place_cnt
                    item['亞'] = second_place_cnt
                    item['季'] = third_place_cnt
                    item['總出賽次數'] = total_race

                elif "最近十個賽馬日" in key or "出賽場數" in key:
                    if len(cells) >= 3 and cells[3]:
                        item['最近十個賽馬日出賽場數'] = cells[3].strip()

                elif "現在位置" in key:
                    location_text = row.css('td:last-child::text').get()
                    if location_text:
                        item['現在位置'] = location_text.strip()

                elif "進口日期" in key:
                    item['進口日期'] = value


        # Right table scraping
        right_table = response.css('table.table_top_right.table_eng_text')[-1]
        right_rows = right_table.css('tbody tr')

        for row in right_rows:
            cells = row.css('td::text').getall()
            links = row.css('td a')
            
            if len(cells) >= 3:
                key = cells[0].strip() if cells[0] else ""
                value = cells[2].strip() if cells[2] else ""
                
                if "練馬師" in key:
                    trainer_text = row.css('td:last-child a::text').get()
                    item['練馬師'] = trainer_text if trainer_text else value
                elif "馬主" in key:
                    owner_text = row.css('td:last-child a::text').get()
                    item['馬主'] = owner_text if owner_text else value
                elif "現時評分" in key:
                    item['現時評分'] = value
                elif "季初評分" in key:
                    item['季初評分'] = value
                elif "父系" in key and not '同父系馬' in key:
                    sire_text = row.css('td:last-child a::text').get()
                    item['父系'] = sire_text.strip() if sire_text else value
                elif "母系" in key:
                    item['母系'] = value
                elif "外祖父" in key:
                    item['外祖父'] = value

        print(item)

        yield {
            'type': 'horse_profile', 
            'data': item 
        }