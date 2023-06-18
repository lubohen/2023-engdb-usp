import boto3
import requests
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # URL do arquivo raw no GitHub
    url = 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_daily_reports/01-01-2021.csv' 


    # Faça uma solicitação GET para obter o conteúdo do arquivo
    response = requests.get(url)

    # Verifique se a solicitação foi bem-sucedida
    if response.status_code == 200:
        # Especifique o bucket S3 e o nome do arquivo
        bucket_name = 'usp-prjint-covid19-raw'
        file_name = '01-01-2021.csv'

        # Baixe o arquivo e salve-o localmente
        with open(file_name, 'wb') as f:
            f.write(response.content)

        # Carregue o arquivo para o bucket S3
        with open(file_name, "rb") as data:
            s3.upload_fileobj(data, bucket_name, file_name)

        # Remova o arquivo local depois de carregá-lo
        os.remove(file_name)

    else:
        print(f'Failed to download file from {url}')

    return {
        'statusCode': 200,
        'body': 'Lambda function completed successfully'
    }
