import pyarrow.parquet as pq
import pandas as pd
import os

def create_dir_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory created: {directory_path}")
    else:
        print(f"Directory already exists: {directory_path}")


def read_all_parquet_from_path(ospath):
    dataset = pq.ParquetDataset(ospath)
    table = dataset.read()
    df = table.to_pandas()
    return df


def merge_race_DFs(
            race_resultDF, 
            race_corunDF,
            race_sectionalDF,
            common_keys = ['date', 'venue', 'race_no', '馬匹編號'],
            corun_sel_col = ['date', 'venue', 'race_no', '馬匹編號', '配備', '走勢評述', ],
            sectional_sel_col = ['date', 'venue', 'race_no', '馬匹編號',
                '第1段_位置', '第1段_距離', '第1段_時間', '第1段_前半', '第1段_後半', 
                '第2段_位置', '第2段_距離', '第2段_時間', '第2段_前半', '第2段_後半', 
                '第3段_位置', '第3段_距離', '第3段_時間', '第3段_前半', '第3段_後半', 
                '第4段_位置', '第4段_距離', '第4段_時間', '第4段_前半', '第4段_後半', 
                '第5段_位置', '第5段_距離', '第5段_時間', '第5段_前半', '第5段_後半', 
                '第6段_位置', '第6段_距離', '第6段_時間', '第6段_前半', '第6段_後半']
    ):
    # First, merge race_resultDF with race_corunDF
    interim_df = pd.merge(
        race_resultDF,
        race_corunDF[corun_sel_col],
        on=common_keys,
        how='left',  # or 'inner' depending on your needs
    )

    # Then merge the result with race_sectionalDF
    final_df = pd.merge(
        interim_df,
        race_sectionalDF[sectional_sel_col],
        on=common_keys,
        how='left',  # or 'inner' depending on your needs
    )

    return final_df
