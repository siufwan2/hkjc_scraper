import scrapy
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import configparser
import re

class HKJC_Horse_Spider(scrapy.Spider):
    name = "hkjc_horse_spider"
    allowed_domains = ["racing.hkjc.com"]
    start_urls = ["https://racing.hkjc.com"]

    def __init__(self, *args, **kwargs):
        super(HKJC_Horse_Spider, self).__init__(*args, **kwargs)

        ''' Init : Read the config '''
        config = configparser.ConfigParser()
        config.read('./scrapy.cfg')
        self.race_result_base_url = config['input']['race_result_base_url']
        self.race_hkjc_base_url = config['input']['race_hkjc_base_url']
        self.output_base_path = config['output']['race_output_base_path']
        self.scrape_method = config['input']['scrape_method']

        self.logger.info(f'The scraped data will be stored to path: {self.output_base_path}')

        ''' Init : form race date urls list to scrape '''
        if self.scrape_method == 'day_range':
            # Using config with string dates
            self.start_date = datetime.strptime(config['input']['start_date'], '%Y-%m-%d')
            self.end_date = datetime.strptime(config['input']['end_date'], '%Y-%m-%d')

            # Generate all dates between start and end (inclusive)
            date_range = []
            current_date = self.start_date
            while current_date <= self.end_date:
                date_range.append(current_date)
                current_date += timedelta(days=1)

            # Create URLs for each date
            self.race_date_urls = [
                {date.strftime("%Y_%m_%d"): f"{self.race_result_base_url}?RaceDate={date.strftime('%Y/%m/%d')}"} 
                for date in date_range
            ]

        # To be run automatically in pipeline
        elif self.scrape_method == 'day_range':
            today = datetime.now()
            past_X_days = [(today - relativedelta(days=i + 1)) for i in range(int(config['input']['past_x_day']))]
            self.race_date_urls = [
                {date.strftime("%Y_%m_%d"): f"{self.race_result_base_url}?RaceDate={date.strftime('%Y/%m/%d')}"} for date in past_X_days
            ]

        self.logger.info(len(self.race_date_urls), 'urls to check')
