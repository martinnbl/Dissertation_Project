import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account

def lambda_handler(event, context):
    """
    AWS Lambda function to scan contracts and process payments
    """
    
    # Initialize BigQuery client with service account
    credentials_info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_KEY'])
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])
    
    if event.get('action') == 'scan_contracts':
        return scan_contracts(client)
    elif event.get('action') == 'process_payments':
        return process_payments(client)
    else:
        return scan_and_process_all(client)

def scan_contracts(client):
    """Scan for contracts ready for payment"""
    
    query = """
    SELECT 
        c.contract_id,
        COALESCE(c.final_amount, c.total_fee) as amount,
        c.currency,
        COUNT(pmo.post_id) as posts_completed,
        c.post_required,
        c.compliance
    FROM `proof-of-brand.social_media_metrics.contract` c
    LEFT JOIN `proof-of-brand.social_media_metrics.post_metrics_objectives` pmo ON c.contract_id = pmo.contract_id
    WHERE c.payment_status = 'Pending'
    AND c.contract_status = 'active'
    AND c.contract_id NOT IN (SELECT contract_id FROM `proof-of-brand.social_media_metrics.payment_queue` WHERE status != 'FAILED')
    GROUP BY c.contract_id, c.total_fee, c.final_amount, c.currency, c.post_required, c.payment_due_days, c.compliance
    HAVING COUNT(pmo.post_id) >= c.post_required
    AND DATE_ADD(MAX(pmo.actual_post_date), INTERVAL c.payment_due_days DAY) <= CURRENT_DATE()
    """
    
    contracts = client.query(query).result()
    
    processed_count = 0
    for contract in contracts:
        insert_query = f"""
        INSERT INTO `proof-of-brand.social_media_metrics.payment_queue` (contract_id, amount, currency, status, created_at)
        VALUES ('{contract.contract_id}', {contract.amount}, '{contract.currency}', 'PENDING', CURRENT_DATETIME())
        """
        client.query(insert_query)
        processed_count += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Contracts scanned successfully',
            'contracts_processed': processed_count
        })
    }

def process_payments(client):
    """Process payments from queue"""
    
    query = """
    SELECT contract_id, amount, currency 
    FROM `proof-of-brand.social_media_metrics.payment_queue`
    WHERE status = 'PENDING'
    ORDER BY created_at ASC
    LIMIT 5
    """
    
    payments = client.query(query).result()
    
    processed_count = 0
    for payment in payments:
        success = call_payment_service(payment.contract_id, payment.amount, payment.currency)
        
        if success:
            client.query(f"""
            UPDATE `proof-of-brand.social_media_metrics.payment_queue`
            SET status = 'COMPLETED', processed_at = CURRENT_DATETIME()
            WHERE contract_id = '{payment.contract_id}'
            """)
            
            client.query(f"""
            UPDATE `proof-of-brand.social_media_metrics.contract`
            SET payment_status = 'Paid'
            WHERE contract_id = '{payment.contract_id}'
            """)
            processed_count += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Payments processed successfully',
            'payments_processed': processed_count
        })
    }

def scan_and_process_all(client):
    """Combined function for simplicity"""
    scan_result = scan_contracts(client)
    process_result = process_payments(client)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'scan_result': json.loads(scan_result['body']),
            'process_result': json.loads(process_result['body'])
        })
    }

def call_payment_service(contract_id, amount, currency):
    """
    Mock payment service for proof of concept
    """
    print(f"💰 Processing payment: Contract {contract_id}, Amount: {amount} {currency}")
    return True  # Simulate success for proof of concept