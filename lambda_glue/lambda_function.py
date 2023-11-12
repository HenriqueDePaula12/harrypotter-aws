import boto3

CRAWLER_NAME = 'crawler_harry'
S3_TARGET = 's3://bucket-harry-potter-characters/DELIVERY/'
DATABASE_NAME = 'hp-results'
REGION_NAME = 'us-east-1'

def create_glue_database(database_name, region_name):
    glue_client = boto3.client('glue', region_name=region_name)
    
    try:
        response = glue_client.create_database(
            DatabaseInput={'Name': database_name}
        )
        print(f'Banco de dados "{database_name}" criado com sucesso no AWS Glue Data Catalog.')
    except Exception:
        print(f'A database "{database_name}" já existe.')

def create_glue_crawler(crawler_name, database_name, s3_target, region_name):
    glue_client = boto3.client('glue', region_name=region_name)
    
    try:
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role='arn:aws:iam::XXXXXXXXX:role/glue-crawler-role', 
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': s3_target}]}
        )
        print(f'Crawler "{crawler_name}" criado com sucesso no AWS Glue.')
    except glue_client.exceptions.AlreadyExistsException:
        print(f'O crawler "{crawler_name}" já existe.')
    except Exception as e:
        print(f'Erro ao criar o crawler: {str(e)}')

def lambda_handler(event=None, context=None):

    create_glue_database(DATABASE_NAME, REGION_NAME)

    create_glue_crawler(CRAWLER_NAME, DATABASE_NAME, S3_TARGET, REGION_NAME)

    print('Executado com Sucesso')
