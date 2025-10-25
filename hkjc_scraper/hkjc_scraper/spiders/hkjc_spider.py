import scrapy
from datetime import datetime
from dateutil.relativedelta import relativedelta
import configparser
import os
import re

import pandas as pd
from snownlp import SnowNLP

class HKJC_Spider(scrapy.Spider):
    name = "hkjc_spider"

    def __init__(self, *args, **kwargs):
        super(HKJC_Spider, self).__init__(*args, **kwargs)
        ''' Step 1: Read the config '''
        config = configparser.ConfigParser()
        config.read('./scrapy.cfg')
        self.race_base_url = config['input']['race_base_url']
        self.race_href_base_url = config['input']['race_href_base_url']
        # self.output_base_path = config['output']['race_output_base_path']

        ''' Step 2: form race date urls list to scrape '''
        today = datetime.now()
        past_X_days = [(today - relativedelta(days=i + 1)) for i in range(int(config['input']['past_x_day']))]
        self.race_date_urls = [
            {date.strftime("%Y_%m_%d"): f"{self.race_base_url}?RaceDate={date.strftime('%Y/%m/%d')}"} for date in past_X_days
        ]

    def analyze_sentiment(self, text):
        """
        Analyze Chinese text sentiment and return score + category as separate values
        """
        try:
            if pd.isna(text) or text == '':
                return None, '無資料'
            
            s = SnowNLP(str(text))
            score = s.sentiments
            
            # Classify sentiment based on score
            if score >= 0.7:
                sentiment = '強烈正面'
            elif score >= 0.55:
                sentiment = '正面'
            elif score >= 0.45:
                sentiment = '中性'
            elif score >= 0.3:
                sentiment = '負面'
            else:
                sentiment = '強烈負面'
            
            return score, sentiment
        
        except Exception as e:
            print(f"Error: {e}")
            return None, '分析錯誤'
        
    def add_date_venue_race_num_to_df(self, df, response_meta):
        df['date'] = response_meta.get('date_hyphen')
        df['venue'] = response_meta.get('venue')
        df['race_no'] = response_meta.get('race_no')
        return df

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
    
    ''' Step 3: Filter those date that actually has race and scrape all race detail from it'''
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

                all_race = [f"{self.race_href_base_url}/{prefix}" \
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

    ''' Step 4: Race detail main page'''
    def race_detail_page(self, response):

        # ================================== 場地資訊 ================================== #
        race_info = response.css('div.race_tab table tbody')

        venue_data = {
            '班次': '未知班次',
            '距离': '未知距离',
            'ratio': '未知范围',
            '賽事級別': '未知級別',
            '場地狀況': '未知',
            '賽道': '未知',
            '時間': '未知',
            '分段時間': '未知'
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
                if distance_match and venue_data['距离'] == '未知距离':  # Only update if still default
                    venue_data['距离'] = distance_match.group(0)

                # 范围 #
                pattern_ratio = r'\([^)]+\)'
                ratio_match = re.search(pattern_ratio, cell)
                if ratio_match and venue_data['ratio'] == '未知范围':  # Only update if still default
                    venue_data['ratio'] = ratio_match.group(0)

                # Pattern for capturing race classes like 二級賽, 一級賽, 三級賽, etc.
                advance_race_class = r'([一二三四五六七八九十]+級賽)\s*-\s*(\d+米)'
                race_class_match = re.search(advance_race_class, cell)
                
                if race_class_match and venue_data['賽事級別'] == '未知級別':  # Only update if still default
                    venue_data['賽事級別'] = race_class_match.group(1)
                    # Also set distance if not already set (and still default)
                    if venue_data['距离'] == '未知距离':
                        venue_data['距离'] = race_class_match.group(2)

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

            elif key == '時間':
                venue_data['時間'] = ', '.join(cells[semi_column_idx:])

            elif key == '分段時間':
                venue_data['分段時間'] = ', '.join(cells[semi_column_idx:])

            # else:
            #     print(f'new key found: {key}')


        df_venue_data = self.add_date_venue_race_num_to_df(pd.DataFrame([venue_data]), response.meta)
        print(df_venue_data.to_dict(orient='records'))

        # ================================== 名次資訊 ================================== #
        # Extract the HTML for the table
        # race_results = response.css('table.f_tac.table_bd.draggable').get()
        race_results = response.xpath('//*[@id="innerContent"]/div[2]/div[5]/table').get()

        # Use pandas to read the HTML table
        df_race_result = pd.read_html(race_results)[0]

        # Replace with normal space
        df_race_result['馬名'] = df_race_result['馬名'].str.replace('\xa0', ' ', regex=False)

        df_race_result = self.add_date_venue_race_num_to_df(df_race_result, response.meta)
        

        print(df_race_result.head().to_dict(orient='records'))

        # ================================ 競賽事件報告 ================================ #
        # Extract the HTML for the incidents table
        # race_incidents = response.css('table.f_tac.table_bd').get() 
        race_incidents = response.xpath('//*[@id="innerContent"]/div[2]/div[7]/table').get() 

        
        # Use pandas to read the HTML table
        df_race_incidents = pd.read_html(race_incidents)[0]

        # Apply the sentiment analysis function
        df_race_incidents[['sentiment_score', 'sentiment_category']] = df_race_incidents['競賽事件'].apply(
            lambda x: pd.Series(self.analyze_sentiment(x))
        )

        df_race_incidents = self.add_date_venue_race_num_to_df(df_race_incidents, response.meta)
        df_race_incidents['馬名'] = df_race_incidents['馬名'].str.replace('\xa0', ' ', regex=False)

        print(df_race_incidents.head().to_dict(orient='records'))


        # ================================ 勝出馬匹血統 ================================ #
        # Extract the horse bloodline table 
        # win_horse_blood = response.css('table.hourse_breed_tab.f_tac')
        win_horse_blood = response.xpath('//*[@id="innerContent"]/div[2]/table')

        # Extract horse name, paternal lineage, and maternal lineage
        horse_name = win_horse_blood.css('a.local::text').get()
        parental_info = win_horse_blood.css('p::text').getall()

        # Split the paternal and maternal lineage
        father = parental_info[0].split(': ')[1] if len(parental_info) > 0 else None
        mother = parental_info[1].split(': ')[1] if len(parental_info) > 1 else None

        # Create a dictionary
        data = {
            "馬名": [horse_name],
            "父系": [father],
            "母系": [mother]
        }

        # Create a Pandas DataFrame
        df_win_horse_blood = pd.DataFrame(data)
        df_win_horse_blood = self.add_date_venue_race_num_to_df(df_win_horse_blood, response.meta)
        print(df_win_horse_blood.head().to_dict(orient='records'))


        # ================================ 分段時間位置 ================================ #
        race_length_position_href = response.xpath('//*[@id="innerContent"]/div[2]/div[3]/p[2]/a/@href').get()

        full_race_length_position_url = f"{self.race_href_base_url}{race_length_position_href}"

        yield scrapy.Request(
            url=full_race_length_position_url,
            callback=self.race_length_position_page,
            meta={
                'date_hyphen': response.meta.get('date_hyphen'),
                'venue': response.meta.get('venue'),
                'race_no': response.meta.get('race_no'),
            }
        )
        # ================================ 沿途走位評述 ================================ #
        race_gear_comment_href = response.xpath('//*[@id="racerunningpositionphotos"]/p[2]/a/@href').get()
        
        race_gear_comment_url = f"{self.race_href_base_url}{race_gear_comment_href}"

        yield scrapy.Request(
            url=race_gear_comment_url,
            callback=self.race_gear_comment_page,
            meta={
                'date_hyphen': response.meta.get('date_hyphen'),
                'venue': response.meta.get('venue'),
                'race_no': response.meta.get('race_no'),
            }
        )
    
    '''Step5a: 分段時間位置'''
    def race_length_position_page(self, response):
        pass
    
    '''Step5b: 沿途走位評述'''
    def race_gear_comment_page(self, response):
        pass