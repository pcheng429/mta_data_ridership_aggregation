from datetime import datetime
import requests

def download_csv_data_from_api(api_url, data_name, dir_path, ti):
    file_path = generate_file_path(dir_path, data_name)

    response = requests.get(api_url)
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Save the content of the response to a local CSV file
        with open(file_path, "wb") as f:
            f.write(response.content)
        print("CSV file downloaded successfully")
    else:
        print("Failed to download CSV file. Status code:", response.status_code)
    
    ti.xcom_push(key="file_path", value=file_path)

def save_df_to_csv(dir_path, data_name, df, index):
    file_path = generate_file_path(dir_path, data_name)
    df.to_csv(file_path, index=index)
    return file_path

def generate_file_path(dir_path, data_name):
    file_name = "{}_{}.csv".format(data_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    file_path = "{}/{}".format(dir_path, file_name)
    return file_path
