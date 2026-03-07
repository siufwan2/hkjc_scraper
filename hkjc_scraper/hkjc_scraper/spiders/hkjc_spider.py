import scrapy
from datetime import datetime
from dateutil.relativedelta import relativedelta
import configparser
import os
import re
from urllib.parse import urljoin

import pandas as pd
from snownlp import SnowNLP

class HKJC_Spider(scrapy.Spider):
    name = "hkjc_spider"

    def __init__(self, *args, **kwargs):
        super(HKJC_Spider, self).__init__(*args, **kwargs)

        ''' Init : Read the config '''
        config = configparser.ConfigParser()
        config.read('./scrapy.cfg')
        self.race_result_base_url = config['input']['race_result_base_url']
        self.race_hkjc_base_url = config['input']['race_hkjc_base_url']
        self.output_base_path = config['output']['race_output_base_path']
        self.logger.info(f'The scraped data will be stored to path: {self.output_base_path}')

        ''' Init : form race date urls list to scrape '''
        today = datetime.now()
        past_X_days = [(today - relativedelta(days=i + 1)) for i in range(int(config['input']['past_x_day']))]
        self.race_date_urls = [
            {date.strftime("%Y_%m_%d"): f"{self.race_result_base_url}?RaceDate={date.strftime('%Y/%m/%d')}"} for date in past_X_days
        ]

        self.logger.info(len(self.race_date_urls), 'urls to check')

    ''' Entry Point '''
    def start_requests(self):
        for date_race_pairs in self.race_date_urls:
            for date_hyphen, race_date_url in date_race_pairs.items():  # Fixed 'itmes' to 'items'
                yield scrapy.Request(
                    url=race_date_url,
                    callback=self.check_has_races,
                    meta={
                        'date_hyphen': date_hyphen
                    }
                )
    
    ''' Step 1: Filter those date that actually has race and start scraping from them'''
    def check_has_races(self, response):
        # Redirect - overseas race ignored
        if int(response.status) in range(300, 400):
            self.logger.info(f"Redirected to: {response.url}")

        else:
            # No race on that date
            if response.css('div.errorout').get():
                self.logger.info(f"{response.meta['date_hyphen']} has No race")
            # Detected race on that date, require further actions
            else:
                self.logger.info(f"{response.meta['date_hyphen']} has race") 
                # Get all race info
                race_num_table = response.css('table.f_fs12.js_racecard')

                all_race = [f"{self.race_hkjc_base_url}/{prefix}" \
                            for prefix in race_num_table.css('a::attr(href)').getall() \
                            if re.search(r"Racecourse=(ST|HV)&RaceNo=(\d+)", prefix)] # Monitor only Sha Tin and Happy Valley
                
                if all_race:
                    first_race_url = re.sub(r"RaceNo=(\d+)", 'RaceNo=1', all_race[0])
                    all_race.insert(0, first_race_url) # Add the first race bk for better formating and understanding

                    
                    for race_date_detail_url in all_race:
                        yield scrapy.Request(
                            url=race_date_detail_url,
                            callback=self.race_detail_page,
                            meta={
                                'date_hyphen': response.meta.get('date_hyphen'),
                                'venue': re.search(r"Racecourse=(ST|HV)", race_date_detail_url).group(1),
                                'race_no': re.search(r"RaceNo=(\d+)", race_date_detail_url).group(1)
                            }
                        )

    ''' Step 2: Race detail main page'''
    def race_detail_page(self, response):
        # ================================== 1. 場地資訊 ================================== #
        race_info = response.css('div.race_tab table tbody')

        venue_data = {
            '班次': '未知班次',
            '賽道距离': '未知距离',
            '范围': '未知范围',
            '賽事級別': '未知級別',
            '場地狀況': '未知',
            '賽道': '未知',
            # '時間': '未知',
            # '分段時間': '未知'
        }

        for table_row in race_info.css('tr'):
            cells = [cell.strip() for cell in table_row.css('td::text').getall() if cell.strip() !='']
            
            key = None
            semi_column_idx = None

            for cell in cells:
                # Separate regex patterns for each part
                pattern_class = r'第.班'
                class_match = re.search(pattern_class, cell)
                if class_match and venue_data['班次'] == '未知班次':  # Only update if still default
                    venue_data['班次'] = class_match.group(0)

                # 距离 #
                pattern_distance = r'\d+米'
                distance_match = re.search(pattern_distance, cell)
                if distance_match and venue_data['賽道距离'] == '未知距离':  # Only update if still default
                    venue_data['賽道距离'] = distance_match.group(0)

                # 范围 #
                pattern_ratio = r'\([^)]+\)'
                ratio_match = re.search(pattern_ratio, cell)
                if ratio_match and venue_data['范围'] == '未知范围':  # Only update if still default
                    venue_data['范围'] = ratio_match.group(0)

                # Pattern for capturing race classes like 二級賽, 一級賽, 三級賽, etc.
                advance_race_class = r'([一二三四五六七八九十]+級賽)\s*-\s*(\d+米)'
                race_class_match = re.search(advance_race_class, cell)
                
                if race_class_match and venue_data['賽事級別'] == '未知級別':  # Only update if still default
                    venue_data['賽事級別'] = race_class_match.group(1)
                    # Also set distance if not already set (and still default)
                    if venue_data['賽道距离'] == '未知距离':
                        venue_data['賽道距离'] = race_class_match.group(2)

                # 其他資料 #
                if ':' in cell:
                    key = cell.replace(':', '').strip()
                    semi_column_idx = cells.index(cell) + 1
                    break

            # Control what key to be in the data avoiding schema change in future
            if key == '場地狀況':
                venue_data['場地狀況'] = ', '.join(cells[semi_column_idx:])

            elif key == '賽道':
                venue_data['賽道'] = ', '.join(cells[semi_column_idx:])
            
            # elif key == '時間':
            #     venue_data['時間'] =  ', '.join(
            #         [
            #             time.strip('()')
            #             for time in cells[semi_column_idx:]
            #         ]
            #     )

            # elif key == '分段時間':
            #     venue_data['分段時間'] = ', '.join(cells[semi_column_idx:])

        # ================================== 2. 名次資訊 ================================== #

        # Extract all race_result_rows from tbody
        race_result_rows = response.xpath('//table[@class="f_tac table_bd draggable"]/tbody/tr')

        # Store all data
        race_result_table_data = []

        for row in race_result_rows:
            # Extract all td cells in the row
            cells = row.xpath('.//td')
            
            row_data = {}
            
            # Position/Rank (special handling for camera icon)
            position_cell = cells[0]
            # Check if there's a camera link
            if position_cell.xpath('.//a/img'):
                # Get the text after the image
                position = position_cell.xpath('string()').get().strip()
                # Extract just the number
                position = re.search(r'\d+', position).group()
            else:
                position = position_cell.xpath('string()').get().strip()
            row_data['名次'] = position
            
            # Horse Number
            horse_number = cells[1].xpath('string()').get().strip()
            row_data['馬號'] = horse_number
            
            # Horse Name (with link)
            horse_name_cell = cells[2]
            horse_name = horse_name_cell.xpath('.//a/text()').get()
            horse_code = horse_name_cell.xpath('.//text()').getall()[-1].strip().strip('()')
            row_data['馬名'] = horse_name
            row_data['馬匹編號'] = horse_code
            row_data['馬名連結'] = urljoin(self.race_hkjc_base_url, horse_name_cell.xpath('.//a/@href').get())
            
            # Jockey
            jockey_cell = cells[3]
            jockey_name = jockey_cell.xpath('.//a/text()').get()
            jockey_link = jockey_cell.xpath('.//a/@href').get()
            row_data['騎師'] = jockey_name
            row_data['騎師連結'] = urljoin(self.race_hkjc_base_url, jockey_link)
            
            # Trainer
            trainer_cell = cells[4]
            trainer_name = trainer_cell.xpath('.//a/text()').get()
            trainer_link = trainer_cell.xpath('.//a/@href').get()
            row_data['練馬師'] = trainer_name
            row_data['練馬師連結'] = urljoin(self.race_hkjc_base_url, trainer_link)
            
            # Actual Weight
            row_data['實際負磅'] = cells[5].xpath('string()').get().strip()
            
            # Declared Weight
            row_data['排位體重'] = cells[6].xpath('string()').get().strip()
            
            # Draw (Gate)
            row_data['檔位'] = cells[7].xpath('string()').get().strip()
            
            # Winning Distance
            row_data['頭馬距離'] = cells[8].xpath('string()').get().strip()
            
            # Running Position (沿途走位)
            position_divs = cells[9].xpath('.//div[@style]/text()').getall()
            # Clean up positions (remove whitespace)
            positions = [pos.strip() for pos in position_divs if pos.strip()]
            row_data['沿途走位'] = '-'.join(positions) if positions else None
            
            # Finish Time
            row_data['完成時間'] = cells[10].xpath('string()').get().strip()
            
            # Odds
            row_data['獨贏賠率'] = cells[11].xpath('string()').get().strip()
            
            race_result_table_data.append(row_data)

        race_result_table_data = self.helper_add_date_venue_race_num_to_data(race_result_table_data, response.meta)


        # ================================ 3. 競賽事件報告 ================================ #
        # Get all rows from tbody
        incident_rows = response.xpath('//table[@class="f_tac table_bd"]/tbody/tr')

        # Store all incident data
        incidents_data = []

        for row in incident_rows:
            # Extract all cells
            cells = row.xpath('.//td')
            
            incident_record = {}
            
            # Position (名次)
            position = cells[0].xpath('string()').get().strip()
            incident_record['名次'] = position
            
            # Horse Number (馬號)
            horse_number = cells[1].xpath('string()').get().strip()
            incident_record['馬號'] = horse_number
            
            # Horse Name (馬名) - with link and code
            horse_cell = cells[2]
            horse_name = horse_cell.xpath('.//a/text()').get()
            horse_link = horse_cell.xpath('.//a/@href').get()
            
            # Extract horse code from the text in parentheses
            horse_text = horse_cell.xpath('string()').get()
            horse_code_match = re.search(r'\(([^)]+)\)', horse_text)
            horse_code = horse_code_match.group(1) if horse_code_match else None
            
            incident_record['馬名'] = horse_name
            incident_record['馬匹編號'] = horse_code
            incident_record['馬名連結'] = horse_link
            
            # Race Incident (競賽事件)
            incident_text = cells[3].xpath('string()').get().strip()
            # Clean up whitespace
            incident_text = ' '.join(incident_text.split())
            incident_record['競賽事件'] = incident_text
            
            # Check for veterinary supplements
            incidents_data.append(incident_record)
        

        for result_row in race_result_table_data:
            # MERGE venue, rank, date
            for key, value in venue_data.items():
                result_row[key] = value

            # MERGE 競賽事件 info to the corresponding row
            for incidents_row in incidents_data:
                if result_row['馬匹編號'] == incidents_row['馬匹編號']:
                    result_row['競賽事件'] = incidents_row['競賽事件']

        self.logger.info(f"race result sample data: {response.meta.get('date_hyphen')} - race {response.meta.get('race_no')} \n {race_result_table_data[0]}")

        yield {
            'type': 'race result',
            'data': race_result_table_data
        }


        # ================================ 4a. 沿途走位評述 ================================ #
        race_gear_comment_href = response.xpath('//*[@id="racerunningpositionphotos"]/p[2]/a/@href').get()
        
        race_gear_comment_url = f"{self.race_hkjc_base_url}{race_gear_comment_href}"

        yield scrapy.Request(
            url=race_gear_comment_url,
            callback=self.race_corunning,
            meta={
                'date_hyphen': response.meta.get('date_hyphen'),
                'venue': response.meta.get('venue'),
                'race_no': response.meta.get('race_no'),
            }
        )

        # ================================ 4b. 分段時間位置 ================================ #
        race_length_position_href = response.xpath('//*[@id="innerContent"]/div[2]/div[3]/p[2]/a/@href').get()

        full_race_length_position_url = f"{self.race_hkjc_base_url}{race_length_position_href}"

        yield scrapy.Request(
            url=full_race_length_position_url,
            callback=self.race_sectional_time,
            meta={
                'date_hyphen': response.meta.get('date_hyphen'),
                'venue': response.meta.get('venue'),
                'race_no': response.meta.get('race_no'),
            }
        )
    
    '''Step4a: 沿途走位評述 '''
    def race_corunning(self, response):
        # Get all rows from tbody
        performance_rows = response.xpath('//table[@class="table_bd f_fs13"]/tbody/tr')

        # Store all performance data
        corunning_data = []

        for row in performance_rows:
            # Extract all cells
            cells = row.xpath('.//td')
            
            performance_record = {}
            
            # Position (名次)
            position = cells[0].xpath('string()').get().strip()
            performance_record['名次'] = position
            
            # Horse Number (馬號)
            horse_number = cells[1].xpath('string()').get().strip()
            performance_record['馬號'] = horse_number
            
            # Horse Name (馬名) - with link and code
            horse_cell = cells[2]
            horse_name = horse_cell.xpath('.//a/text()').get().strip()
            horse_link = horse_cell.xpath('.//a/@href').get()
            
            # Extract horse code from parentheses
            horse_text = horse_cell.xpath('string()').get()
            horse_code_match = re.search(r'\(([^)]+)\)', horse_text)
            horse_code = horse_code_match.group(1) if horse_code_match else None
            
            performance_record['馬名'] = horse_name
            performance_record['馬匹編號'] = horse_code
            performance_record['馬名連結'] = urljoin(self.race_hkjc_base_url, horse_link)
            
            # Jockey (騎師)
            jockey = cells[3].xpath('string()').get().strip()
            performance_record['騎師'] = jockey
            
            # Gear/Equipment (配備)
            gear = cells[4].xpath('string()').get().strip()
            performance_record['配備'] = gear if gear != '--' else None
            
            # Performance Review (走勢評述)
            review = cells[5].xpath('string()').get().strip()
            # Clean up whitespace
            review = ' '.join(review.split())
            performance_record['走勢評述'] = review
            
            corunning_data.append(performance_record)

        corunning_data = self.helper_add_date_venue_race_num_to_data(corunning_data, response.meta)
        self.logger.info(f"corunning sample data: {response.meta.get('date_hyphen')} - race {response.meta.get('race_no')} \n {corunning_data[0]}")
        
        yield {
            'type': 'race corunning result',
            'data': corunning_data
        }

    '''Step5b: 分段時間位置 '''
    def race_sectional_time(self, response):

        # Get all rows from tbody
        sectional_rows = response.xpath('//table[@class="table_bd f_tac race_table"]/tbody/tr')

        # Store all sectional data
        sectional_data = []

        for row in sectional_rows:
            # Extract basic info (first 3 columns + last column)
            cells = row.xpath('.//td')
            
            # Basic information
            finish_order = cells[0].xpath('string()').get().strip()
            horse_number = cells[1].xpath('string()').get().strip()
            
            # Horse name with link - CLEANED VERSION
            horse_cell = cells[2]
            
            # Get the full text from the cell
            full_text = horse_cell.xpath('string()').get()
            
            # Method 1: Extract just the Chinese characters (works for all Chinese names)
            # This regex matches Chinese characters only
            chinese_name_match = re.search(r'([\u4e00-\u9fff]+)', full_text)
            if chinese_name_match:
                horse_name_clean = chinese_name_match.group(1)
            else:
                # Method 2: Remove the code in parentheses and clean up
                horse_name_clean = re.sub(r'\s*\([^)]+\)', '', full_text)
                horse_name_clean = horse_name_clean.replace('\xa0', '').strip()
            
            # print(f"Original: {full_text.strip()} -> Cleaned: {horse_name_clean}")
            
            horse_link = urljoin(self.race_hkjc_base_url, horse_cell.xpath('.//a/@href').get())
            
            # Extract horse code from text (store separately)
            horse_code_match = re.search(r'\(([^)]+)\)', full_text)
            horse_code = horse_code_match.group(1) if horse_code_match else None
            
            # Final time
            finish_time = cells[-1].xpath('string()').get().strip()
            
            # Initialize horse record with all 6 segments
            horse_record = {
                '過終點次序': finish_order,
                '馬號': horse_number,
                '馬名': horse_name_clean,  # This will now be just the Chinese name
                '馬匹編號': horse_code,
                '馬名連結': horse_link,
                '完成時間': finish_time,
            }
            
            # Initialize all 6 segments with None values
            for section_num in range(1, 7):
                horse_record[f'第{section_num}段_位置'] = None
                horse_record[f'第{section_num}段_距離'] = None
                horse_record[f'第{section_num}段_時間'] = None
                horse_record[f'第{section_num}段_前半'] = None
                horse_record[f'第{section_num}段_後半'] = None
            
            # Process each sectional (columns 3 to 8, which are indices 3-8 in 0-based indexing)
            for section_idx in range(3, 9):  # Columns for sections 1-6
                section_num = section_idx - 2  # Convert to 1-based section number (1-6)
                
                if section_idx < len(cells):
                    section_cell = cells[section_idx]
                    
                    # Check if it's an image (blank) - sections 5 and 6 are typically blank
                    if section_cell.xpath('.//img'):
                        # Keep default None values for this section
                        continue
                    
                    # Extract position and distance
                    position_elem = section_cell.xpath('.//span[@class="f_fl"]/text()').get()
                    distance_elem = section_cell.xpath('.//i/text()').get()
                    
                    # Extract section time
                    time_elem = section_cell.xpath('.//p[not(@class)]/text()').get()
                    if not time_elem:
                        time_elem = section_cell.xpath('.//p[@class="sectional_200"]/text()').get()
                    
                    # Extract 200m split times if available
                    split_times = section_cell.xpath('.//span[@class="color_blue2"]/span/text()').getall()
                    split_times = [t.strip() for t in split_times if t.strip()]
                    
                    # Populate the data for this section
                    if position_elem:
                        horse_record[f'第{section_num}段_位置'] = position_elem.strip()
                    if distance_elem:
                        horse_record[f'第{section_num}段_距離'] = distance_elem.strip()
                    if time_elem:
                        horse_record[f'第{section_num}段_時間'] = time_elem.strip()
                    if split_times and len(split_times) == 2:
                        horse_record[f'第{section_num}段_前半'] = split_times[0]
                        horse_record[f'第{section_num}段_後半'] = split_times[1]
            
            sectional_data.append(horse_record)

        sectional_data = self.helper_add_date_venue_race_num_to_data(sectional_data, response.meta)
        self.logger.info(f"sectional time sample data: {response.meta.get('date_hyphen')} - race {response.meta.get('race_no')} \n {sectional_data[0]}")

        yield {
            'type': 'race sectional time result',
            'data': sectional_data
        }

    ''' HELPER FUNCTIONS '''
    def helper_add_date_venue_race_num_to_data(self, data, response_meta):
        if isinstance(data, dict):
            # self.logger.info('Is dict')
            data['date'] = response_meta.get('date_hyphen')
            data['venue'] = response_meta.get('venue')
            data['race_no'] = response_meta.get('race_no')

        elif isinstance(data, list):
            # self.logger.info('Is list of dict')
            for dict_item in data:
                dict_item['date'] = response_meta.get('date_hyphen')
                dict_item['venue'] = response_meta.get('venue')
                dict_item['race_no'] = response_meta.get('race_no')

        return data