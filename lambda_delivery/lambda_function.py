import pandas as pd
import awswrangler as wr
import boto3

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

BUCKET_NAME = 'bucket-harry-potter-characters'

def change_columns_name_to_pt_br(df):

    colunas_em_portugues = {
        "ID": "ID",
        "NAME": "NOME",
        "ALTERNATE_NAMES": "NOMES_ALTERNATIVOS",
        "SPECIES": "ESPECIE",
        "GENDER": "GENERO",
        "HOUSE": "CASA",
        "DATEOFBIRTH": "DATA_DE_NASCIMENTO",
        "YEAROFBIRTH": "ANO_DE_NASCIMENTO",
        "WIZARD": "BRUXO",
        "ANCESTRY": "ASCENDENCIA",
        "EYECOLOUR": "COR_DOS_OLHOS",
        "HAIRCOLOUR": "COR_DO_CABELO",
        "WAND": "VARINHA",
        "PATRONUS": "PATRONO",
        "HOGWARTSSTUDENT": "ESTUDANTE_DE_HOGWARTS",
        "HOGWARTSSTAFF": "EQUIPE_DE_HOGWARTS",
        "ACTOR": "ATOR",
        "ALTERNATE_ACTORS": "ATORES_ALTERNATIVOS",
        "ALIVE": "VIVO",
        "IMAGE": "IMAGEM"
    }

    df.rename(columns=colunas_em_portugues, inplace=True)
    return df
    
def enriquecer_dataframe(df):

    df["ANO_DE_NASCIMENTO"] = df["ANO_DE_NASCIMENTO"].fillna(0).astype(int)

    df['IDADE'] = df.apply(lambda row: 2023 - row['ANO_DE_NASCIMENTO'] if row['ANO_DE_NASCIMENTO'] != 0 else None, axis=1)
    df['IDADE'] = df["IDADE"].fillna(0).astype(int)

    df["DATA_DE_NASCIMENTO"] = df["DATA_DE_NASCIMENTO"].fillna("")
    df["DATA_DE_NASCIMENTO"] = pd.to_datetime(df["DATA_DE_NASCIMENTO"])
    df["DATA_DE_NASCIMENTO"] = df["DATA_DE_NASCIMENTO"].astype(str).replace('NaT', '')

    df["DESCRICAO_PERSONAGEM"] = df["NOME"] + " é um bruxo " + df["ESPECIE"] + " da casa " + df["CASA"] + " em Hogwarts."

    df['DESCRICAO_VARINHA'] = df['VARINHA'].apply(lambda x: f"A varinha tem um núcleo de {x['core']} com {x['length']} polegadas de comprimento e é feita de madeira de {x['wood']}." if x and x.get('core') and x.get('length') and x.get('wood') else "Descrição não disponível")

    df["DESCRICAO_PATRONUS"] = "O Patronus de " + df["NOME"] + " assume a forma de um(a) " + df["PATRONO"] + "."

    return df

def save_enriched_characters(bucket_name, df):

    return wr.s3.to_parquet(
        df=df,
        path=f's3://{bucket_name}/DELIVERY/harry_data_delivery.snappy.parquet'
    )

def lambda_handler(event=None, context=None):

    df = wr.s3.read_parquet(f's3://{BUCKET_NAME}/RAW/harry_data_characters.snappy.parquet')

    df = change_columns_name_to_pt_br(df)

    df = enriquecer_dataframe(df)

    save_enriched_characters(BUCKET_NAME, df)
    print('Executado com Sucesso')