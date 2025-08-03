import os
import json
import logging
import requests
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize helpers - will be imported by main Lambda function
openai_helper = None
bigquery_helper = None

def set_helpers(oa_helper, bq_helper):
    """Set the helper instances from the main Lambda function"""
    global openai_helper, bigquery_helper
    openai_helper = oa_helper
    bigquery_helper = bq_helper

def format_for_bigquery(extracted_metrics, user_info, raw_response):
    """Format the extracted metrics into the correct BigQuery schema format"""
    
    # Base influencer information - fix the username extraction
    username = user_info.get('username', 'unknown')
    if username == 'unknown' and extracted_metrics.get('influencer_name'):
        username = extracted_metrics.get('influencer_name')
    
    # If still unknown, try to extract from raw_response
    if username == 'unknown':
        import re
        handle_match = re.search(r'@(\w+)', raw_response)
        if handle_match:
            username = handle_match.group(1)
    
    collection_timestamp = datetime.now().isoformat()
    
    # Format recent posts for BigQuery nested structure
    recent_posts = []
    for post in extracted_metrics.get('recent_posts', []):
        post_record = {
            'post_id': post.get('post_id', post.get('url', '')),
            'likes': post.get('likes', 0),
            'comments': post.get('comments', 0),
            'post_date': post.get('post_date', collection_timestamp[:10]),  # YYYY-MM-DD format
            'caption': post.get('caption', '')[:100] if post.get('caption') else None
        }
        recent_posts.append(post_record)
    
    # Create single BigQuery record that matches the schema
    bigquery_record = {
        "influencer_name": username,  # This matches the required schema field
        "collection_date": collection_timestamp,
        "followers_count": extracted_metrics.get('followers_count'),
        "following_count": extracted_metrics.get('following_count'),
        "posts_count": extracted_metrics.get('posts_count'),
        "engagement_rate": extracted_metrics.get('engagement_rate'),
        "avg_likes_per_post": extracted_metrics.get('avg_likes_per_post'),
        "avg_comments_per_post": extracted_metrics.get('avg_comments_per_post'),
        "recent_posts": recent_posts,  # Nested array as expected by schema
        "story_metrics": extracted_metrics.get('story_metrics'),
        "audience_demographics": extracted_metrics.get('audience_demographics'),
        "processed_at": collection_timestamp,  # Required field, always has value
        "data_source": "telegram_bot",
        "processing_version": "1.0",
        "error": extracted_metrics.get('error')
    }
    
    return bigquery_record  # Return single record, not array

def send_telegram_message(chat_id, text, parse_mode='HTML'):
    """Send a message to Telegram chat"""
    try:
        TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': parse_mode
        }
        response = requests.post(url, json=data)
        return response.json()
    except Exception as e:
        logging.error(f"Error sending message: {e}")
        return None

def process_influencer_response(message_text, chat_id, user_info):
    """Process metrics data provided by influencer - UPDATED FOR AGENCY COMPLIANCE"""
    try:
        global openai_helper, bigquery_helper
        
        if not openai_helper:
            send_telegram_message(chat_id, "❌ Bot is not properly configured. Please try again later.")
            return
        
        # NEW: Use the enhanced processing workflow
        result = openai_helper.process_influencer_message(
            message_text, 
            user_info.get('username', 'unknown')
        )
        
        # Extract the components from the result
        classification = result.get('classification', {})
        metrics_data = result.get('metrics_data')
        response_data = result.get('response', {})
        
        # Send the agency-compliant response immediately
        agency_response = response_data.get('response_message', 
            "Thanks for your message! We'll get back to you shortly 😊")
        
        send_telegram_message(chat_id, agency_response)
        
        # Check if escalation is needed
        if response_data.get('escalation_needed', False):
            # Log for human review
            logging.warning(f"Escalation needed for @{user_info.get('username', 'unknown')}: {response_data.get('internal_notes', '')}")
        
        # Process metrics if they were provided
        if metrics_data and metrics_data.get('has_metrics', False):
            # Send processing message (with agency tone)
            send_telegram_message(
                chat_id, 
                "Perfect! We're processing your data now 🔄"
            )
            
            # Structure the data for BigQuery
            structured_data = format_for_bigquery(metrics_data, user_info, message_text)
            
            # Store in BigQuery (only if available)
            success = False
            if bigquery_helper and bigquery_helper != "failed":
                success = bigquery_helper.insert_metrics(structured_data)
            
            if success:
                # Count posts for display
                post_count = len(structured_data.get('recent_posts', []))
                
                # Safe value extraction
                influencer_name = structured_data.get('influencer_name', 'Unknown')
                followers_count = structured_data.get('followers_count')
                engagement_rate = structured_data.get('engagement_rate')
                data_quality = metrics_data.get('data_quality_score', 0)
                
                # Format values safely
                followers_text = f"{followers_count:,}" if followers_count is not None else "N/A"
                engagement_text = f"{engagement_rate}%" if engagement_rate is not None else "N/A"
                quality_text = f"{data_quality:.1f}/1.0" if data_quality else "N/A"
                
                # Agency-compliant success message
                success_message = f"""Everything's in — thank you so much 🙏

<b>📊 Your metrics have been processed:</b>
• Influencer: @{influencer_name}
• Posts analyzed: {post_count}
• Followers: {followers_text}
• Engagement rate: {engagement_text}
• Data quality: {quality_text}

We'll review and get back to you with any follow-up questions. Usually takes 1-2 business days for full analysis."""
                
                send_telegram_message(chat_id, success_message)
                
            else:
                # Agency-compliant error message
                send_telegram_message(
                    chat_id, 
                    "Thanks for sending this! We're having a technical issue storing the data — we'll sort this out and get back to you shortly 😊"
                )
        
        # Handle different message intents with appropriate follow-up
        elif classification.get('message_intent') == 'metrics_unavailable':
            # Already sent appropriate response above, no additional action needed
            pass
        
        elif classification.get('message_intent') == 'question':
            # Already sent appropriate response, but might need follow-up
            if response_data.get('follow_up_required'):
                logging.info(f"Follow-up required for @{user_info.get('username')} by {response_data.get('follow_up_date')}")
        
        elif classification.get('message_intent') == 'delay_notification':
            # Already sent supportive response, log for timeline adjustment
            logging.info(f"Delay notification from @{user_info.get('username')}")
        
        elif classification.get('message_intent') == 'payment_inquiry':
            # Should have been escalated above
            logging.info(f"Payment inquiry from @{user_info.get('username')} - escalated")
        
        else:
            # For general_chat or unclear messages, the response was already sent
            pass
    
    except Exception as e:
        logging.error(f"Error processing influencer response: {e}")
        # Agency-compliant error response
        send_telegram_message(
            chat_id, 
            "Thanks for your message! We're experiencing a technical issue — we'll get back to you shortly 😊"
        )

def handle_admin_command(message_text, chat_id, user_info):
    """Handle admin commands - ENHANCED WITH AGENCY TONE"""
    message_text = message_text.strip()
    
    if message_text.lower() == '/start':
        welcome_message = f"""Hello {user_info.get('first_name', 'there')}! 👋

<b>🤖 Instagram Metrics Collection</b>

<b>📊 For Influencers:</b>
• Just send your metrics when we reach out
• Screenshots or text format both work
• We'll help format everything properly

<b>💡 How it works:</b>
1. Send your Instagram metrics data
2. Our AI processes and structures the data
3. You get confirmation when complete

Type /help for more information!"""
        
        send_telegram_message(chat_id, welcome_message)
    
    elif message_text.lower() == '/help':
        help_message = """<b>🔧 How to use this bot:</b>

<b>📊 For Influencers:</b>
• We'll reach out with clear requests
• Send metrics in any format you prefer
• Screenshots of Instagram insights work great
• Don't worry about perfect formatting

<b>✨ What makes this special:</b>
• AI extracts data from informal responses
• Agency-appropriate tone in all communications
• Quality scoring for data reliability
• Automatic escalation for complex issues

<b>📋 Typical metrics we collect:</b>
• Follower/following counts
• Recent post performance
• Engagement rates
• Story views (if available)

Need help? Just ask! 🤝"""
        
        send_telegram_message(chat_id, help_message)
    
    else:
        # If not a command, might be an influencer providing metrics
        process_influencer_response(message_text, chat_id, user_info)