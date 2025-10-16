import scrapy
from datetime import datetime
from dateutil.relativedelta import relativedelta
import configparser
import os
import re

class HKJC_Spider(scrapy.Spider):
    name = "hkjc_spider"

    def __init__(self, *args, **kwargs):
        super(HKJC_Spider, self).__init__(*args, **kwargs)
        ''' Step 1: Read the config '''
        config = configparser.ConfigParser()
        config.read('./scrapy.cfg')
        self.race_base_url = config['input']['race_base_url']
        self.race_href_base_url = config['input']['race_href_base_url']
        self.output_base_path = config['output']['race_output_base_path']

        ''' Step 2: form race date urls list to scrape '''
        today = datetime.now()
        past_1_yr = [(today - relativedelta(days=i + 1)) for i in range(365)]
        self.race_date_urls = [
            {date.strftime("%Y_%m_%d"): f"{self.race_base_url}?RaceDate={date.strftime('%Y/%m/%d')}"} for date in past_1_yr
        ]

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
                
                all_race.insert(0, response.url) # Add the first race bk for better formating and understanding
                
                for race_date_detail_url in all_race:
                    yield scrapy.Request(
                        url=race_date_detail_url,
                        callback=self.race_detail_page
                    )

    ''' Step 4: Race detail main page'''
    def race_detail_page(self, response):
        pass