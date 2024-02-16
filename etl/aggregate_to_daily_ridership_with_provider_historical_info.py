import pandas as pd

from etl.commons.data_io import save_df_to_csv

def aggregate_data(dir_path, new_data_name, ti):

    ridership_file_processed_path = ti.xcom_pull(task_ids="process_mta_ridership_data", key="file_path")
    wifi_location_processed_file_path = ti.xcom_pull(task_ids="process_mta_wifi_location_data", key="file_path")

    ridership_processed_df = pd.read_csv(ridership_file_processed_path)
    wifi_location_processed_df = pd.read_csv(wifi_location_processed_file_path)

    # Each total daily ridership for the date is joining the historical and provider info by their matching station_complex
    aggregated_df  = ridership_processed_df.merge(wifi_location_processed_df, on = "station_complex", how = "left")

    save_df_to_csv(dir_path, new_data_name, aggregated_df, False)
