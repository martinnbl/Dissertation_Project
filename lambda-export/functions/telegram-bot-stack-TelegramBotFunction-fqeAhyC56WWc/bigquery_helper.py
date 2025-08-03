import os
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from dotenv import load_dotenv
import boto3

load_dotenv()

class BigQueryHelper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        try:
            # Get from Parameter Store
            self.logger.info("Getting service account from Parameter Store")
            ssm = boto3.client('ssm')
            response = ssm.get_parameter(
                Name='/telegram-bot/google-service-account',
                WithDecryption=True
            )
            service_account_json = response['Parameter']['Value']
            self.logger.info("Successfully retrieved service account from Parameter Store")
            
            # Parse and create credentials
            credentials_info = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/bigquery']
            )
            
            # Initialize client
            self.client = bigquery.Client(credentials=credentials)
            self.table_id = os.getenv('BQ_TABLE_ID')
            self.logger.info("BigQuery client initialized successfully")
            
            # Ensure table exists
            self._ensure_table_exists()
            
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery helper: {e}")
            raise e
    
    def _ensure_table_exists(self):
        """Create the table if it doesn't exist"""
        try:
            self.client.get_table(self.table_id)
            self.logger.info(f"Table {self.table_id} already exists")
        except NotFound:
            self.logger.info(f"Creating table {self.table_id}")
            self._create_table()
    
    def _create_table(self):
        """Create the Instagram metrics table with proper schema"""
        schema = [
            bigquery.SchemaField("influencer_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("collection_date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("recent_posts", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("post_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("likes", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("comments", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("post_date", "DATE", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table = bigquery.Table(self.table_id, schema=schema)
        table = self.client.create_table(table)
        self.logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    
    def insert_metrics(self, structured_data):
        """Insert structured Instagram metrics into BigQuery"""
        try:
            collection_date = datetime.now().isoformat()
            processed_at = datetime.now().isoformat()
            
            # Transform recent posts
            recent_posts = []
            for post in structured_data.get('recent_posts', []):
                if isinstance(post, dict):
                    post_data = {
                        'post_id': post.get('post_id'),
                        'likes': post.get('likes'),
                        'comments': post.get('comments'),
                        'post_date': post.get('post_date', datetime.now().strftime('%Y-%m-%d')),
                    }
                    recent_posts.append(post_data)
            
            # Build the BigQuery row
            bq_row = {
                'influencer_name': structured_data.get('influencer_name'),
                'collection_date': collection_date,
                'recent_posts': recent_posts,
                'processed_at': processed_at,
            }
            
            # Insert the row
            errors = self.client.insert_rows_json(
                self.client.get_table(self.table_id), 
                [bq_row]
            )
            
            if errors:
                self.logger.error(f"BigQuery insert errors: {errors}")
                return False
            
            self.logger.info(f"Successfully inserted metrics for {structured_data.get('influencer_name')}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting metrics: {e}")
            return False
    
    def get_influencer_metrics(self, influencer_name, days_back=30):
        """Retrieve recent metrics for an influencer"""
        return []
    
    def get_metrics_summary(self, influencer_name=None):
        """Get a summary of metrics collected"""
        return {}
