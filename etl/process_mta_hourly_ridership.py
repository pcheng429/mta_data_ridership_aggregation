import pandas as pd

from etl.commons.data_io import save_df_to_csv

def process_mta_hourly_ridership(dir_path, new_data_name, ti):
    file_path = ti.xcom_pull(task_ids="ingest_mta_hourly_ridership_data", key="file_path")

    # station_complex_id contains small portion of non-numerical id, to avoid warning and efficiency
    df = pd.read_csv(file_path, dtype={'station_complex_id': str})
    # Reduce the transit_timestamp to transit_date
    df['transit_date'] = pd.to_datetime(df['transit_timestamp']).dt.date

    # Step 1: group the hourly ridership by the date and the station_complex
    # Step 2: sum up the total ridership in the same date and in the same sation_complex
    procssed = df.groupby(["transit_date", "station_complex"])['ridership'].sum()

    new_file_path = save_df_to_csv(dir_path, new_data_name, procssed, True)
    
    ti.xcom_push(key="file_path", value=new_file_path)
