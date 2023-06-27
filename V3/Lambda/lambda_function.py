import requests
import urllib
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Obtenha a árvore de arquivos do repositório
    response = requests.get('https://api.github.com/repos/nytimes/covid-19-data/git/trees/master?recursive=1') #https://api.github.com/repos/nytimes/covid-19-data/git/trees/master?recursive=1
    tree = response.json()['tree']

    # Filtrar para arquivos CSV no diretório desejado
    csv_files = [file for file in tree if file['path'] and file['path'].endswith('.csv')] #startswith('/covid-19-data/') and file['path']

    bucket_name = 'usp-prjint-covid'
    s3_folder = 'raw/nytimes/'
    base_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/'
    
    # Lista todos os objetos no bucket e pasta
    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)
    existing_files = [item['Key'] for item in s3_objects.get('Contents', [])]

    for file in csv_files:
        file_name = file['path'].split('/')[-1]
        key = s3_folder + file_name
        
        # Verifica se o arquivo já existe no bucket
        if key in existing_files:
            print(f'{file_name} already exists in {bucket_name}/{s3_folder}')
            continue

        url = base_url + file['path']
        with urllib.request.urlopen(url) as response:
            file_content = response.read()
            s3.put_object(Body=file_content, Bucket=bucket_name, Key=key)
            print(f'{file_name} uploaded to {bucket_name}/{s3_folder}')

    return {
        'statusCode': 200,
        'body': 'Files uploaded to S3!'
    }