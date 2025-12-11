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

def handler(event, context):
    try:
        logger.info("Получение списка книг")
        
        params = event.get('queryStringParameters', {}) or {}
        search_query = params.get('search', '').strip()
        
        logger.info(f"Поисковый запрос: '{search_query}'")

        driver = init_ydb_driver()
        session = driver.table_client.session().create()
        
        if search_query:
            search_escaped = search_query.replace("'", "''").lower()
            query = f"""
            SELECT book_id, title, author, description, file_format, file_path_in_s3 FROM books WHERE (title LIKE '%{search_escaped}%' OR title LIKE '%{search_escaped.lower()}%' OR title LIKE '%{search_escaped.upper()}%' OR author LIKE '%{search_escaped}%' OR author LIKE '%{search_escaped.lower()}%' OR author LIKE '%{search_escaped.upper()}%') ORDER BY title"""
        else:
            query = """
            SELECT book_id, title, author, description, file_format, file_path_in_s3 
            FROM books 
            ORDER BY title
            """
        
        logger.info(f"Выполняем запрос: {query[:200]}...")

        result_sets = session.transaction().execute(
            query,
            commit_tx=True
        )

        books = []
        for row in result_sets[0].rows:
            book = {
                'book_id': str(getattr(row, 'book_id', '')),
                'title': str(getattr(row, 'title', '')),
                'author': str(getattr(row, 'author', '')),
                'description': str(getattr(row, 'description', '')),
                'file_format': str(getattr(row, 'file_format', '')),
                'file_path_in_s3': str(getattr(row, 'file_path_in_s3', ''))
            }
            books.append(book)
        
        logger.info(f"Найдено книг: {len(books)}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(books)
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения книг: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': 'Ошибка сервера'})
        }