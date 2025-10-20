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
        # data = []
        
        # for row in race_info.css('tr'):
        #     cells = row.css('td::text').getall()
        #     if cells:  # Ensure the row contains <td> elements
        #         data.append([cell.strip() for cell in cells])

        # venue_dict = {}
        
        # # Extract 班, 米, ratio from row 1
        # if data[1][0]:
        #     parts = data[1][0].split(' - ')
        #     venue_dict['班'] = parts[0] if len(parts) > 0 else ''
        #     venue_dict['米'] = parts[1].replace('米', '') if len(parts) > 1 else ''
        #     venue_dict['ratio'] = parts[2] if len(parts) > 2 else ''
        
        # # Extract 場地狀況 from row 1
        # venue_dict['場地狀況'] = data[1][2] if len(data[1]) > 2 else ''
        
        # # Extract 讓賽 from row 2
        # if data[2][0]:
        #     venue_dict['讓賽'] = data[2][0].replace('讓賽', '')
        
        # # Extract 賽道 from row 2
        # venue_dict['賽道'] = data[2][2] if len(data[2]) > 2 else ''
        
        # # Extract HKD from row 3
        # if data[3][0]:
        #     hkd_match = re.search(r'[\d,]+', data[3][0])
        #     venue_dict['HKD'] = hkd_match.group() if hkd_match else ''
        
        # # Extract 時間 from row 3
        # venue_dict['時間'] = [item for item in data[3][2:] if item]
        
        # # Not necessary - Extract 分段時間 from row 4
        # # venue_dict['分段時間'] = [item[:5] for item in data[4][2:] if item]

        # df_venue = pd.DataFrame([{
        #     '班': venue_dict.get('班', ''),
        #     '米': venue_dict.get('米', ''),
        #     'ratio': venue_dict.get('ratio', ''),
        #     '場地狀況': venue_dict.get('場地狀況', ''),
        #     '讓賽': venue_dict.get('讓賽', ''),
        #     '賽道': venue_dict.get('賽道', ''),
        #     '時間': ' '.join(venue_dict.get('時間', [])),
        #     # '分段時間': ' '.join(venue_dict.get('分段時間', [])),
        #     'HKD': venue_dict.get('HKD', '')
        # }])

        # print(df_venue.head())

        # ================================== 名次資訊 ================================== #
        # Extract the HTML for the table
        # race_results = response.css('table.f_tac.table_bd.draggable').get()
        race_results = response.xpath('//*[@id="innerContent"]/div[2]/div[5]/table').get()

        # Use pandas to read the HTML table
        df_race_result = pd.read_html(race_results)[0]

        df_race_result = self.add_date_venue_race_num_to_df(df_race_result, response.meta)
        

        print(df_race_result.head())

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

        print(df_race_incidents.head())


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
        print(df_win_horse_blood.head())


        # Pass the data to pipelines
        pass
        # yield {
        # }