import pandas as pd
import numpy as np

from etl.commons.data_io import save_df_to_csv

def process_mta_wifi_location(dir_path, new_data_name, ti):
    file_path = ti.xcom_pull(task_ids="ingest_mta_wifi_location_data", key="file_path")
    df = pd.read_csv(file_path)

    # if any column of "at_t","sprint","t_mobile","verizon" is true, then the provider_available will be true 
    conditions = [(df["at_t"] == "Yes") | (df["sprint"]=="Yes") |( df["t_mobile"]=="Yes")|(df["verizon"]=="Yes")]
    outputs = [True]
    df["provider_available"] = np.select(conditions, outputs, False)

    # if any station under a station_complex is "historical", the station_complex is assumed historical
    df["historical_complex"] = df.groupby("station_complex")["historical"].transform(lambda x: (x == "Yes").any())

    #remove dumplicate station complex and save the essential columns
    df = df.drop_duplicates(subset=['station_complex'], keep="first")
    processed = df[["station_complex", "historical_complex","provider_available"]]
    processed = processed.rename(columns={'historical_complex': 'historical'}) 

    new_file_path = save_df_to_csv(dir_path, new_data_name, processed, False)

    ti.xcom_push(key="file_path", value=new_file_path)
