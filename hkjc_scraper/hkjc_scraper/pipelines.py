# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
import os
import numpy as np


class HkjcScraperPipeline:
    """Simple pipeline that saves each race as individual Parquet files with consistent schema"""
    
    def open_spider(self, spider):
        """Initialize when spider opens"""
        self.spider = spider
        self.spider_type = getattr(spider, 'spider_type', spider.name)

        if self.spider_type == 'race':
            self.race_output_base_path  = spider.output_base_path 
            
            # Create output directory if it doesn't exist
            if not os.path.exists(self.race_output_base_path ):
                os.makedirs(self.race_output_base_path )
                spider.logger.info(f"Created output directory: {self.race_output_base_path }")
            
            # Define schema for each data type
            self.schemas = {
                'race_sectional': {
                    # Basic info
                    '過終點次序': 'string',
                    '馬號': 'string',
                    '馬名': 'string',
                    '馬匹編號': 'string',
                    '馬名連結': 'string',
                    '完成時間': 'string',
                    'date': 'string',
                    'venue': 'string',
                    'race_no': 'string',
                    
                    # Section 1
                    '第1段_位置': 'string',
                    '第1段_距離': 'string',
                    '第1段_時間': 'string',
                    '第1段_前半': 'string',
                    '第1段_後半': 'string',
                    
                    # Section 2
                    '第2段_位置': 'string',
                    '第2段_距離': 'string',
                    '第2段_時間': 'string',
                    '第2段_前半': 'string',
                    '第2段_後半': 'string',
                    
                    # Section 3
                    '第3段_位置': 'string',
                    '第3段_距離': 'string',
                    '第3段_時間': 'string',
                    '第3段_前半': 'string',
                    '第3段_後半': 'string',
                    
                    # Section 4
                    '第4段_位置': 'string',
                    '第4段_距離': 'string',
                    '第4段_時間': 'string',
                    '第4段_前半': 'string',
                    '第4段_後半': 'string',
                    
                    # Section 5
                    '第5段_位置': 'string',
                    '第5段_距離': 'string',
                    '第5段_時間': 'string',
                    '第5段_前半': 'string',
                    '第5段_後半': 'string',
                    
                    # Section 6
                    '第6段_位置': 'string',
                    '第6段_距離': 'string',
                    '第6段_時間': 'string',
                    '第6段_前半': 'string',
                    '第6段_後半': 'string',
                },
                'race_result': {
                    # Define schema for race results
                    '名次': 'string',
                    '馬號': 'string',
                    '馬名': 'string',
                    '馬匹編號': 'string',
                    '馬名連結': 'string',
                    '騎師': 'string',
                    '騎師連結': 'string',
                    '練馬師': 'string',
                    '練馬師連結': 'string',
                    '實際負磅': 'string',
                    '排位體重': 'string',
                    '檔位': 'string',
                    '頭馬距離': 'string',
                    '沿途走位': 'string',
                    '完成時間': 'string',
                    '獨贏賠率': 'string',
                    'date': 'string',
                    'venue': 'string',
                    'race_no': 'string',
                    '班次': 'string',
                    '賽道距离': 'string',
                    '范围': 'string',
                    '賽事級別': 'string',
                    '場地狀況': 'string',
                    '賽道': 'string',
                    '競賽事件': 'string',
                },
                'race_corunning': {
                    # Define schema for corunning results
                    '名次': 'string',
                    '馬號': 'string',
                    '馬名': 'string',
                    '馬匹編號': 'string',
                    '馬名連結': 'string',
                    '騎師': 'string',
                    '配備': 'string',
                    '走勢評述': 'string',
                    'date': 'string',
                    'venue': 'string',
                    'race_no': 'string',
                }
            }

        elif self.spider_type == 'horse':
            self.horse_output_path  = spider.horse_output_path 

            # Create output directory if it doesn't exist
            if not os.path.exists(self.horse_output_path ):
                os.makedirs(self.horse_output_path )
                spider.logger.info(f"Created output directory: {self.horse_output_path }")
            self.schemas = {
                'horse_profile': {
                    '馬名': 'string',
                    '馬匹編號': 'string',
                    '出生地': 'string',
                    '馬齡': 'string',
                    '毛色': 'string',
                    '性別': 'string',
                    '進口類別': 'string',
                    '今季獎金': 'string',
                    '總獎金': 'string',
                    '冠': 'string',
                    '亞': 'string',
                    '季': 'string',
                    '總出賽次數': 'string',
                    '最近十個賽馬日出賽場數': 'string',
                    '現在位置': 'string',
                    '進口日期': 'string',
                    '現時評分': 'string',
                    '季初評分': 'string',
                    '父系': 'string',
                    '母系': 'string',
                    '外祖父': 'string',
                }
            }
    
    def process_item(self, item, spider):
        """Process each item - save immediately as Parquet with consistent schema"""
        if self.spider_type == 'race':
            adapter = ItemAdapter(item)
            
            item_type = adapter.get('type')
            date_hyphen = adapter.get('date_hyphen')
            venue = adapter.get('venue')
            race_no = adapter.get('race_no')
            data = adapter.get('data')
            
            if not all([item_type, date_hyphen, venue, race_no, data]):
                spider.logger.warning(f"Missing required fields in item: {item}")
                return item
            
            if not isinstance(data, list):
                spider.logger.warning(f"Data is not a list: {type(data)}")
                return item
            
            # Map the type string to directory name and schema key
            type_map = {
                'race result': ('race_result', 'race_result'),
                'race corunning result': ('race_corunning', 'race_corunning'),
                'race sectional time result': ('race_sectional', 'race_sectional')
            }
            
            dir_name, schema_key = type_map.get(item_type, (None, None))
            if not dir_name:
                spider.logger.warning(f"Unknown item type: {item_type}")
                return item
            
            # Create type-specific subdirectory
            type_dir = os.path.join(self.race_output_base_path , dir_name, date_hyphen)
            if not os.path.exists(type_dir):
                os.makedirs(type_dir)
                spider.logger.info(f"Created subdirectory: {type_dir}")
            
            # Create filename
            filename = f"{venue}_{race_no}.parquet"
            filepath = os.path.join(type_dir, filename)
            
            # Convert to DataFrame and enforce schema
            if data:
                df = pd.DataFrame(data)
                
                # Enforce consistent schema
                df = self._enforce_schema(df, schema_key)
                
                # Save as Parquet
                df.to_parquet(filepath, index=False, engine='pyarrow')
                spider.logger.info(f"Saved {len(data)} records to {dir_name}/{filename}")
            else:
                spider.logger.warning(f"No data to save for {filename}")
            
            return item
        
        elif self.spider_type == 'horse':
            adapter = ItemAdapter(item)
            item_type = adapter.get('type')
            data = adapter.get('data')
            
            if item_type != 'horse_profile':
                spider.logger.warning(f"Unknown horse item type: {item_type}")
                return item

            if not isinstance(data, dict):
                spider.logger.warning(f"Data is not a list: {type(data)}")
                return item
            
            horseId = data['馬匹編號']

            # Create type-specific subdirectory
            filename = f"{horseId}.parquet"
            filepath = os.path.join(self.horse_output_path, filename)

            # Convert to DataFrame and enforce schema
            if data:
                df = pd.DataFrame([data])
                
                # Enforce consistent schema
                df = self._enforce_schema(df, schema_key='horse_profile')
                
                # Save as Parquet
                df.to_parquet(filepath, index=False, engine='pyarrow')
                spider.logger.info(f"Saved {len(data)} records to {filepath}")
            else:
                spider.logger.warning(f"No data to save for {filename}")
            
            return item



    def _enforce_schema(self, df, schema_key):
        """Enforce consistent schema on DataFrame"""
        schema = self.schemas.get(schema_key)
        if not schema:
            return df
        
        # Ensure all schema columns exist
        for col in schema.keys():
            if col not in df.columns:
                df[col] = pd.NA  # Add missing columns with null values
        
        # Convert to specified types and replace None with empty string for string columns
        for col, dtype in schema.items():
            if col in df.columns:
                if dtype == 'string':
                    # Convert None to empty string for string columns
                    df[col] = df[col].fillna('').astype('string')
                else:
                    df[col] = df[col].astype(dtype)
        
        # Keep only schema columns in the correct order
        df = df[list(schema.keys())]
        
        return df
    
    def close_spider(self, spider):
        """Clean up when spider closes"""
        spider.logger.info("Pipeline closed. All files saved.")