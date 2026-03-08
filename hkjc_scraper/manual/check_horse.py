from utils import create_dir_if_not_exists, read_all_parquet_from_path, merge_race_DFs
import configparser
import os
import pandas as pd



if __name__=="__main__":
    config = configparser.ConfigParser()
    config.read('./scrapy.cfg')
    
    print('Reading from race DB')

    race_result_path = os.path.join(config['output']['race_output_base_path'], 'race_result')
    race_corun_path = os.path.join(config['output']['race_output_base_path'], 'race_corunning')
    race_sectional_path = os.path.join(config['output']['race_output_base_path'], 'race_sectional')

    race_resultDF = read_all_parquet_from_path(race_result_path)
    race_corunDF = read_all_parquet_from_path(race_corun_path)
    race_sectionalDF = read_all_parquet_from_path(race_sectional_path)

    race_df = merge_race_DFs(race_resultDF, race_corunDF, race_sectionalDF)
    race_df['race_id'] = race_df['date']+"_"+race_df['venue']+"_"+race_df['race_no']

    print('Stats')
    print("number of races", race_df['race_id'].nunique())
    print("number of unique horse", race_df['馬匹編號'].nunique())

    print("Exporting the checking validation")
    horse_checking_output_path = config['output']['horse_checking_output_path']
    horse_output_path = config['output']['horse_output_path']

    create_dir_if_not_exists(horse_checking_output_path)

    horse_checking_output_csv_path = os.path.join(horse_checking_output_path, 'checking.csv')

    if os.path.exists(horse_checking_output_csv_path):
        chk_df = pd.read_csv(horse_checking_output_csv_path)
        # Ensure last_update_date is datetime
        chk_df['last_update_date'] = pd.to_datetime(chk_df['last_update_date'])
    else:
        chk_df = pd.DataFrame({
            '馬匹編號': pd.Series(dtype='str'),
            '馬名': pd.Series(dtype='str'),
            '馬名連結': pd.Series(dtype='str'),  
            'last_update_date': pd.Series(dtype='datetime64[ns]'),
            'need_update': pd.Series(dtype='bool')
        })

    # Get unique horses from race data
    to_update = race_df[['馬匹編號', '馬名', '馬名連結']].drop_duplicates()
    today = pd.Timestamp.now().normalize()  # Normalize to remove time component
    
    # Merge with existing checking data
    merged_df = to_update.merge(chk_df, on=['馬匹編號', '馬名', '馬名連結'], how='left', suffixes=('', '_existing'))
    
    # Determine if update is needed
    merged_df['need_update'] = False
    merged_df['last_update_date'] = merged_df['last_update_date']
    
    # If horse exists, check if it needs update (30 days rule)
    mask_exists = merged_df['last_update_date'].notna()
    days_diff = (today - merged_df.loc[mask_exists, 'last_update_date']).dt.days
    merged_df.loc[mask_exists, 'need_update'] = days_diff >= 30
    
    # For new horses, set need_update to True and set last_update_date
    mask_new = merged_df['last_update_date'].isna()
    merged_df.loc[mask_new, 'need_update'] = True
    merged_df.loc[mask_new, 'last_update_date'] = today
    
    # Update last_update_date for those that need update
    merged_df.loc[merged_df['need_update'], 'last_update_date'] = today
    
    # Select and reorder columns for output
    final_df = merged_df[['馬匹編號', '馬名', '馬名連結', 'last_update_date', 'need_update']]
    
    # Save back to CSV
    final_df.to_csv(horse_checking_output_csv_path, index=False)
    print(f"Updated checking file saved to {horse_checking_output_csv_path}")
    
    # Also save horses that need update to separate file
    horses_to_update = final_df[final_df['need_update']]
    if not horses_to_update.empty:
        update_csv_path = os.path.join(horse_checking_output_path, 'horses_to_update.csv')
        horses_to_update.to_csv(update_csv_path, index=False)
        print(f"Horses needing update saved to {update_csv_path}")

    
