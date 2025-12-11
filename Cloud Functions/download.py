import os
import json
import boto3
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
        logger.info("download handler")
        logger.info(f"Получен event: {json.dumps(event)}")

        params = event.get('queryStringParameters', {}) or {}
        book_id = params.get('bookId')
        
        if not book_id:
            logger.error("Отсутствует bookId в queryStringParameters")
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Не указан ID книги'})
            }
        
        logger.info(f"Запрос на скачивание книги: {book_id}")
        
        driver = init_ydb_driver()
        s3_client = init_s3_client()
        
        session = driver.table_client.session().create()
        
        query = f"""
        SELECT book_id, title, author, file_format, file_path_in_s3 FROM books WHERE book_id = '{book_id}'"""
        
        logger.info(f"Executing query: {query}")

        result_sets = session.transaction().execute(
            query,
            commit_tx=True
        )
        
        if not result_sets[0].rows:
            logger.warning(f"Книга не найдена в YDB: {book_id}")
            return {
                'statusCode': 404,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Книга не найдена'})
            }
        
        result_row = result_sets[0].rows[0]
        
        logger.info(f"Тип result_row: {type(result_row)}")
        logger.info(f"Атрибуты result_row: {dir(result_row)}")
        
        book_data = {}
        
        if hasattr(result_row, 'book_id'):
            book_data['book_id'] = getattr(result_row, 'book_id', '')
            book_data['title'] = getattr(result_row, 'title', '')
            book_data['author'] = getattr(result_row, 'author', '')
            book_data['file_format'] = getattr(result_row, 'file_format', '')
            book_data['file_path_in_s3'] = getattr(result_row, 'file_path_in_s3', '')
        
        elif isinstance(result_row, dict):
            book_data['book_id'] = result_row.get('book_id', '')
            book_data['title'] = result_row.get('title', '')
            book_data['author'] = result_row.get('author', '')
            book_data['file_format'] = result_row.get('file_format', '')
            book_data['file_path_in_s3'] = result_row.get('file_path_in_s3', '')
        
        elif isinstance(result_row, (tuple, list)):
            # Предполагаем порядок: book_id, title, author, file_format, file_path_in_s3
            if len(result_row) >= 5:
                book_data['book_id'] = result_row[0]
                book_data['title'] = result_row[1]
                book_data['author'] = result_row[2]
                book_data['file_format'] = result_row[3]
                book_data['file_path_in_s3'] = result_row[4]
        
        if not book_data.get('title'):
            logger.error(f"Не удалось извлечь данные. Структура result_row: {result_row}")
            return {
                'statusCode': 500,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Ошибка формата данных'})
            }
        
        def decode_if_bytes(value):
            if isinstance(value, bytes):
                return value.decode('utf-8', errors='ignore')
            return str(value)
        
        book = {
            'book_id': decode_if_bytes(book_data['book_id']),
            'title': decode_if_bytes(book_data['title']),
            'author': decode_if_bytes(book_data['author']),
            'file_format': decode_if_bytes(book_data['file_format']),
            'file_path_in_s3': decode_if_bytes(book_data['file_path_in_s3'])
        }
        
        logger.info(f"Найдена книга: {book['title']}")
        
        download_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': 'main-library-books',
                'Key': book['file_path_in_s3'],
                'ResponseContentDisposition': f'attachment; filename="{book["title"]}.{book["file_format"]}"'
            },
            ExpiresIn=900
        )
        
        logger.info(f"Сгенерирована ссылка для скачивания")
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'download_url': download_url,
                'filename': f"{book['title']}.{book['file_format']}",
                'book_title': book['title'],
                'author': book['author'],
                'expires_in': 900
            })
        }
        
    except Exception as e:
        logger.error(f"Ошибка скачивания книги: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Ошибка скачивания: {str(e)}'})
        }