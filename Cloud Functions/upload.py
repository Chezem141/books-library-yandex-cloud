import os
import json
import boto3
import uuid
import ydb
import ydb.iam
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def init_ydb_driver():
    endpoint = os.environ['YDB_ENDPOINT']
    database = os.environ['YDB_DATABASE']
    
    credentials = ydb.iam.MetadataUrlCredentials()
    driver_config = ydb.DriverConfig(
        endpoint=endpoint,
        database=database,
        credentials=credentials
    )
    
    driver = ydb.Driver(driver_config)
    driver.wait(fail_fast=True, timeout=5)
    return driver

def init_s3_client():
    return boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='ru-central1'
    )

def handler(event, context):
    try:
        logger.info("Начало загрузки книги")
        
        body = json.loads(event['body'])
        logger.info(f"Полученные данные: {json.dumps(body)}")
        
        required_fields = ['title', 'author', 'file_format', 'file_name']
        for field in required_fields:
            if not body.get(field):
                logger.warning(f"Отсутствует обязательное поле: {field}")
                return {
                    'statusCode': 400,
                    'headers': {'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': f'Отсутствует поле: {field}'})
                }
        
        book_id = f"book_{uuid.uuid4().hex}"
        file_name = body['file_name'].replace(' ', '_')
        file_path = f"books/{book_id}/{file_name}"
        
        logger.info(f"Создание книги: ID: {book_id}, {body['title']}, {body['author']}, {body['description']}, Файл: {file_name}")
        
        driver = init_ydb_driver()
        s3_client = init_s3_client()

        try:
            s3_client.head_bucket(Bucket='main-library-books')
            logger.info("Бакет main-library-books доступен")
        except Exception as e:
            logger.error(f"Ошибка доступа к бакету: {str(e)}")
            return{
                'statusCode': 500,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f'Ошибка доступа к хранилищу: {str(e)}'})
            }
        
        session = driver.table_client.session().create()
        
        inserted_title = body['title']
        inserted_author = body['author']
        inserted_description = body.get('description')
        inserted_file_format = body['file_format']

        query = f"""
        INSERT INTO books (book_id, title, author, description, file_format, file_path_in_s3) VALUES ('{book_id}', '{inserted_title}', '{inserted_author}', '{inserted_description}', '{inserted_file_format}', '{file_path}')"""
        
        logger.info(f"Executing query: {query}")

        session.transaction().execute(
            query,
            commit_tx=True
        )
        
        upload_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': 'main-library-books',
                'Key': file_path,
                'ContentType': get_content_type(body['file_format'])
            },
            ExpiresIn=3600  # 1 час
        )
        
        logger.info(f"Книга создана, URL для загрузки файла сгенерирован: {upload_url}, бакет: main-library-books, key: {file_path}")
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'success': True,
                'book_id': book_id,
                'upload_url': upload_url,
                'file_path': file_path,
                'message': 'Метаданные книги сохранены. Используйте upload_url для загрузки файла.'
            })
        }
        
    except Exception as e:
        logger.error(f"Ошибка загрузки книги: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Ошибка создания книги: {str(e)}'})
        }

def get_content_type(file_format):
    types = {
        'pdf': 'application/pdf',
        'epub': 'application/epub+zip',
        'djvu': 'image/vnd.djvu',
        'txt': 'text/plain',
        'doc': 'application/msword',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    }
    return types.get(file_format.lower(), 'application/octet-stream')