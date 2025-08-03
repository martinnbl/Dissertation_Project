import json
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for helpers (initialized later)
openai_helper = None
bigquery_helper = None

def get_helpers():
    """Initialize helpers only when needed"""
    global openai_helper, bigquery_helper
    
    if openai_helper is None:
        try:
            from openai_helper import OpenAIHelper
            openai_helper = OpenAIHelper()
            logger.info("OpenAI helper initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI helper: {e}")
            openai_helper = "failed"
    
    if bigquery_helper is None:
        try:
            from bigquery_helper import BigQueryHelper
            bigquery_helper = BigQueryHelper()
            logger.info("BigQuery helper initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery helper: {e}")
            bigquery_helper = "failed"
    
    return openai_helper, bigquery_helper

def lambda_handler(event, context):
    """AWS Lambda handler"""
    try:
        logger.info(f"Received event: {event}")
        
        if 'httpMethod' in event:
            if event['httpMethod'] == 'GET' and event.get('path') == '/health':
                return handle_health_check(event, context)
            elif event['httpMethod'] == 'POST' and event.get('path') == '/webhook':
                return handle_webhook(event, context)
        
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'Not found'})
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def handle_health_check(event, context):
    """Health check endpoint"""
    try:
        # Try to get helpers but don't fail if they don't work
        openai_ok = bool(os.getenv('OPENAI_API_KEY'))
        telegram_ok = bool(os.getenv('TELEGRAM_TOKEN'))
        bigquery_ok = bool(os.getenv('BQ_TABLE_ID'))
        
        # Test if helpers can be initialized
        try:
            oa_helper, bq_helper = get_helpers()
            openai_status = oa_helper != "failed"
            bigquery_status = bq_helper != "failed"
        except Exception:
            openai_status = False
            bigquery_status = False
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'status': 'healthy',
                'timestamp': context.aws_request_id,
                'environment_variables': {
                    'openai': openai_ok,
                    'telegram': telegram_ok,
                    'bigquery': bigquery_ok
                },
                'services': {
                    'openai_helper': openai_status,
                    'bigquery_helper': bigquery_status
                }
            })
        }
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'partial',
                'error': str(e),
                'timestamp': context.aws_request_id
            })
        }

def handle_webhook(event, context):
    """Handle Telegram webhook"""
    try:
        # Get helpers
        oa_helper, bq_helper = get_helpers()
        
        # Import webhook functions and set helpers
        from webhook_functions import (
            set_helpers,
            handle_admin_command
        )
        
        # Set the helpers for the webhook functions
        set_helpers(oa_helper, bq_helper)
        
        body = json.loads(event['body']) if event.get('body') else {}
        logger.info(f"Received webhook data: {body}")
        
        if 'message' in body:
            message = body['message']
            chat_id = message['chat']['id']
            user_info = message.get('from', {})
            text = message.get('text', '').strip()
            
            logger.info(f"Message from {user_info.get('username', 'unknown')} ({chat_id}): {text}")
            
            if text:
                handle_admin_command(text, chat_id, user_info)
            else:
                from webhook_functions import send_telegram_message
                send_telegram_message(chat_id, "👋 Hello! Send your Instagram metrics when requested.")
        
        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'ok'})
        }
    
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'message': str(e)})
        }
