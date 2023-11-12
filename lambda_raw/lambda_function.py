import requests
import pandas as pd
import awswrangler as wr

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)
# pd.set_option('display.width', 1000)

BUCKET_NAME = 'bucket-harry-potter-characters'

def get_data():

    url = 'https://hp-api.onrender.com/api/characters'
    data_list = []

    while url:
        r = requests.get(url)
        data = r.json()
        data_list.extend(data)
        url = r.links.get('next', {}).get('url')

    df = pd.DataFrame(data_list)
    df.columns = df.columns.str.upper()
    return df

def save_characters_s3(bucket_name, df):

    return wr.s3.to_parquet(
        df=df,
        path=f's3://{bucket_name}/RAW/harry_data_characters.snappy.parquet'
    )

def lambda_handler(event=None, context=None):

    df = get_data()

    save_characters_s3(BUCKET_NAME, df)
    print('Executado com Sucesso')