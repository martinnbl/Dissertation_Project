import os
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

load_dotenv()

class BigQueryHelper:
    def __init__(self):
        self.client = bigquery.Client()
        self.table_id = os.getenv('BQ_TABLE_ID')  # Format: project.dataset.table
        self.logger = logging.getLogger(__name__)
        
        # Ensure table exists
        self._ensure_table_exists()
    
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
            bigquery.SchemaField("followers_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("following_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("posts_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("engagement_rate", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("avg_likes_per_post", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("avg_comments_per_post", "INTEGER", mode="NULLABLE"),
            
            # Recent posts as repeated record
            bigquery.SchemaField("recent_posts", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("post_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("likes", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("comments", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("post_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("caption", "STRING", mode="NULLABLE"),
            ]),
            
            # Story metrics as record
            bigquery.SchemaField("story_metrics", "RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("daily_story_views", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("story_completion_rate", "FLOAT", mode="NULLABLE"),
            ]),
            
            # Audience demographics as record
            bigquery.SchemaField("audience_demographics", "RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("top_countries", "STRING", mode="REPEATED"),
                bigquery.SchemaField("age_groups", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("age_18_24", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("age_25_34", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("age_35_44", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("age_45_plus", "FLOAT", mode="NULLABLE"),
                ]),
                bigquery.SchemaField("gender_split", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("male", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("female", "FLOAT", mode="NULLABLE"),
                ]),
            ]),
            
            # Metadata fields
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("data_source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("processing_version", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error", "STRING", mode="NULLABLE"),
        ]
        
        table = bigquery.Table(self.table_id, schema=schema)
        table.description = "Instagram influencer metrics collected via Telegram bot"
        
        table = self.client.create_table(table)
        self.logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    
    def insert_metrics(self, structured_data):
        """
        Insert structured Instagram metrics into BigQuery
        
        Args:
            structured_data (dict): Structured metrics data from OpenAI
            
        Returns:
            bool: Success status
        """
        try:
            # Transform the data for BigQuery format
            bq_row = self._transform_for_bigquery(structured_data)
            
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
    
    def _transform_for_bigquery(self, data):
        """Transform structured data to BigQuery format"""
        # Handle timestamp conversion
        collection_date = data.get('collection_date')
        if isinstance(collection_date, str):
            try:
                # Parse various date formats
                if 'T' in collection_date:
                    dt = datetime.fromisoformat(collection_date.replace('Z', '+00:00'))
                else:
                    dt = datetime.strptime(collection_date, "%Y-%m-%d %H:%M:%S")
                collection_date = dt.isoformat()
            except:
                collection_date = datetime.now().isoformat()
        elif collection_date is None:
            collection_date = datetime.now().isoformat()
        
        # Fix for processed_at - ensure it's never empty
        processed_at = data.get('processed_at')
        if processed_at and isinstance(processed_at, str):
            try:
                dt = datetime.fromisoformat(processed_at.replace('Z', '+00:00'))
                processed_at = dt.isoformat()
            except:
                processed_at = datetime.now().isoformat()
        else:
            # This is the key fix - always set processed_at if it's missing or empty
            processed_at = datetime.now().isoformat()
        
        # Transform recent posts
        recent_posts = []
        for post in data.get('recent_posts', []):
            if isinstance(post, dict):
                post_data = {
                    'post_id': post.get('post_id'),
                    'likes': post.get('likes'),
                    'comments': post.get('comments'),
                    'post_date': post.get('post_date'),
                    'caption': post.get('caption', '')[:100] if post.get('caption') else None
                }
                recent_posts.append(post_data)
        
        # Transform audience demographics
        audience_demographics = None
        if data.get('audience_demographics'):
            demo = data['audience_demographics']
            
            # Handle age groups
            age_groups = None
            if demo.get('age_groups'):
                age_data = demo['age_groups']
                age_groups = {
                    'age_18_24': age_data.get('18-24'),
                    'age_25_34': age_data.get('25-34'),
                    'age_35_44': age_data.get('35-44'),
                    'age_45_plus': age_data.get('45+')
                }
            
            # Handle gender split
            gender_split = None
            if demo.get('gender_split'):
                gender_split = {
                    'male': demo['gender_split'].get('male'),
                    'female': demo['gender_split'].get('female')
                }
            
            audience_demographics = {
                'top_countries': demo.get('top_countries', []),
                'age_groups': age_groups,
                'gender_split': gender_split
            }
        
        # Build the BigQuery row
        bq_row = {
            'influencer_name': data.get('influencer_name'),
            'collection_date': collection_date,
            'followers_count': data.get('followers_count'),
            'following_count': data.get('following_count'),
            'posts_count': data.get('posts_count'),
            'engagement_rate': data.get('engagement_rate'),
            'avg_likes_per_post': data.get('avg_likes_per_post'),
            'avg_comments_per_post': data.get('avg_comments_per_post'),
            'recent_posts': recent_posts,
            'story_metrics': data.get('story_metrics'),
            'audience_demographics': audience_demographics,
            'processed_at': processed_at,  # This will now always have a value
            'data_source': data.get('data_source', 'telegram_bot'),
            'processing_version': data.get('processing_version', '1.0'),
            'error': data.get('error')
        }
        
        return bq_row
    
    def get_influencer_metrics(self, influencer_name, days_back=30):
        """
        Retrieve recent metrics for an influencer
        
        Args:
            influencer_name (str): Name of the influencer
            days_back (int): Number of days to look back
            
        Returns:
            list: Recent metrics records
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.table_id}`
            WHERE influencer_name = @influencer_name
            AND collection_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days_back DAY)
            ORDER BY collection_date DESC
            LIMIT 10
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("influencer_name", "STRING", influencer_name),
                    bigquery.ScalarQueryParameter("days_back", "INT64", days_back)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            self.logger.error(f"Error retrieving metrics: {e}")
            return []
    
    def get_metrics_summary(self, influencer_name=None):
        """
        Get a summary of metrics collected
        
        Args:
            influencer_name (str): Optional filter by influencer
            
        Returns:
            dict: Summary statistics
        """
        try:
            where_clause = ""
            params = []
            
            if influencer_name:
                where_clause = "WHERE influencer_name = @influencer_name"
                params.append(bigquery.ScalarQueryParameter("influencer_name", "STRING", influencer_name))
            
            query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT influencer_name) as unique_influencers,
                MAX(collection_date) as latest_collection,
                MIN(collection_date) as earliest_collection,
                AVG(followers_count) as avg_followers,
                AVG(engagement_rate) as avg_engagement_rate
            FROM `{self.table_id}`
            {where_clause}
            """
            
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            return dict(next(results))
            
        except Exception as e:
            self.logger.error(f"Error getting summary: {e}")
            return {}