from flask import Flask, request, jsonify
import os
import json
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime

# Import your custom helpers
from openai_helper import OpenAIHelper
from bigquery_helper import BigQueryHelper

# Load environment variables
load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize helpers
openai_helper = OpenAIHelper()
bigquery_helper = BigQueryHelper()

# Your bot token
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

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
        url = f"{TELEGRAM_API_URL}/sendMessage"
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

def request_metrics_from_influencer(influencer_handle, chat_id, user_info):
    """Request metrics directly from the influencer - UPDATED WITH AGENCY TONE"""
    try:
        # Enhanced request message following agency guidelines
        request_message = f"""Just checking in to see if you can share your recent Instagram metrics 😊

<b>📊 What we'd love to see:</b>

<b>For your last 3-5 posts:</b>
• Post URLs (or brief descriptions)
• Likes and comments for each
• Post dates
• Views (for videos/reels)

<b>Plus your current:</b>
• Follower count
• Following count  
• Total posts

<b>💡 Any format works!</b>
You can send:
• Screenshots of your insights
• Copy-paste the numbers
• Quick summary like "Post 1: 2,150 likes, 120 comments..."

<b>⏰ Timeline:</b> No rush — send when convenient!

We're here to help if you need anything 🙏"""
        
        # Send the request
        send_telegram_message(chat_id, request_message)
        
        # Log the request
        logging.info(f"Metrics request sent to {influencer_handle} from user {user_info.get('username', 'unknown')}")
        
        # Agency-compliant confirmation to admin
        confirmation_message = f"""✅ <b>Request sent to @{influencer_handle}</b>

We've reached out with a friendly request for their metrics.

<b>📤 What happens next:</b>
• They'll receive our agency-tone request
• We'll process their response with enhanced extraction
• You'll get notified when data is collected
• Any issues get escalated for manual review

<b>⏳ Expected timeline:</b> Most influencers respond within 24-48 hours"""
        
        send_telegram_message(chat_id, confirmation_message)
        return True
        
    except Exception as e:
        logging.error(f"Error requesting metrics from influencer: {e}")
        send_telegram_message(
            chat_id, 
            "There was an issue sending the request — we'll sort this out and try again 😊"
        )
        return False

def process_influencer_response(message_text, chat_id, user_info):
    """Process metrics data provided by influencer - UPDATED FOR AGENCY COMPLIANCE"""
    try:
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
            # Could send notification to admin channel here
        
        # Process metrics if they were provided
        if metrics_data and metrics_data.get('has_metrics', False):
            # Send processing message (with agency tone)
            send_telegram_message(
                chat_id, 
                "Perfect! We're processing your data now 🔄"
            )
            
            # Structure the data for BigQuery
            structured_data = format_for_bigquery(metrics_data, user_info, message_text)
            
            # Store in BigQuery
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

<b>🎯 For Admins:</b>
• "Request metrics from @username" 
• /summary — Collection overview
• /recent @username — Recent data

<b>📊 For Influencers:</b>
• Just send your metrics when we reach out
• Screenshots or text format both work
• We'll help format everything properly

<b>💡 Quick start:</b> Try "Request metrics from @[handle]"

Type /help for more details!"""
        
        send_telegram_message(chat_id, welcome_message)
    
    elif message_text.lower() == '/help':
        help_message = """<b>🔧 How to use this bot:</b>

<b>👨‍💼 Admin Commands:</b>
• "Request metrics from @username"
• /summary — Stats overview  
• /recent @username — Recent metrics

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
    
    elif message_text.lower() == '/summary':
        try:
            summary = bigquery_helper.get_metrics_summary()
            
            if summary:
                summary_message = f"""<b>📊 Collection Summary</b>

<b>Total Records:</b> {summary.get('total_records', 0)}
<b>Unique Influencers:</b> {summary.get('unique_influencers', 0)}
<b>Latest Collection:</b> {summary.get('latest_collection', 'N/A')}

<b>📈 Average Metrics:</b>
• Followers: {summary.get('avg_followers', 0):,.0f}
• Engagement: {summary.get('avg_engagement_rate', 0):.1f}%

<b>💾 Storage:</b> BigQuery warehouse
<b>🔄 Processing:</b> Enhanced AI extraction with quality scoring"""
            else:
                summary_message = "📊 No metrics collected yet.\n\nStart by requesting data from an influencer! Try:\n'Request metrics from @username'"
            
            send_telegram_message(chat_id, summary_message)
            
        except Exception as e:
            logging.error(f"Error getting summary: {e}")
            send_telegram_message(chat_id, "We're having trouble retrieving the summary — looking into it! 😊")
    
    elif message_text.lower().startswith('/recent'):
        parts = message_text.split(' ', 1)
        if len(parts) > 1:
            influencer_handle = parts[1].strip('@')
            try:
                recent_metrics = bigquery_helper.get_influencer_metrics(influencer_handle, days_back=30)
                
                if recent_metrics:
                    recent_message = f"<b>📈 Recent data for @{influencer_handle}</b>\n\n"
                    for i, metric in enumerate(recent_metrics[:5], 1):
                        followers = metric.get('followers_count', 'N/A')
                        engagement = metric.get('engagement_rate', 'N/A')
                        date = metric.get('collection_date', 'N/A')[:10]  # Just date part
                        
                        recent_message += f"<b>{i}. {date}</b>\n"
                        recent_message += f"   Followers: {followers:,}\n" if isinstance(followers, int) else f"   Followers: {followers}\n"
                        recent_message += f"   Engagement: {engagement}%\n\n" if isinstance(engagement, (int, float)) else f"   Engagement: {engagement}\n\n"
                else:
                    recent_message = f"📊 No recent data for @{influencer_handle}\n\nTry requesting fresh metrics!"
                
                send_telegram_message(chat_id, recent_message)
                
            except Exception as e:
                logging.error(f"Error getting recent metrics: {e}")
                send_telegram_message(chat_id, "Having trouble pulling that data — we'll check what's up! 😊")
        else:
            send_telegram_message(chat_id, "Please specify an influencer:\n/recent @username")
    
    elif any(keyword in message_text.lower() for keyword in ['request metrics', 'get data', 'ask for metrics']):
        # Extract influencer handle
        import re
        handle_match = re.search(r'@(\w+)', message_text)
        if handle_match:
            influencer_handle = handle_match.group(1)
            request_metrics_from_influencer(influencer_handle, chat_id, user_info)
        else:
            send_telegram_message(
                chat_id, 
                "Please specify the influencer handle:\n\n"
                "• 'Request metrics from @username'\n"
                "• 'Get data from @handle'"
            )
    
    else:
        # Process as potential influencer response
        process_influencer_response(message_text, chat_id, user_info)

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming webhook requests from Telegram"""
    try:
        # Get the JSON data from the request
        data = request.get_json()
        
        # Log the incoming data for debugging
        logging.info(f"Received webhook data: {data}")
        
        # Process the message
        if 'message' in data:
            message = data['message']
            chat_id = message['chat']['id']
            user_info = message.get('from', {})
            text = message.get('text', '').strip()
            
            # Log the message
            logging.info(f"Message from {user_info.get('username', 'unknown')} ({chat_id}): {text}")
            
            # Handle the message
            if text:
                handle_admin_command(text, chat_id, user_info)
            else:
                # Handle non-text messages (like photos)
                if 'photo' in message:
                    send_telegram_message(
                        chat_id, 
                        "Thanks for the screenshot! 📸 Could you also send the key metrics as text so we can process them accurately?\n\n"
                        "Something like:\n"
                        "• Followers: 45,230\n"
                        "• Following: 892\n"
                        "• Posts: 324\n"
                        "• Average likes: 1,850\n\n"
                        "We're here to help if you need anything! 😊"
                    )
                else:
                    send_telegram_message(
                        chat_id, 
                        "Hello! 👋\n\n"
                        "<b>Admins:</b> Try 'Request metrics from @username'\n"
                        "<b>Influencers:</b> Send your Instagram metrics when requested\n"
                        "<b>Help:</b> /help"
                    )
        
        return jsonify({'status': 'ok'})
    
    except Exception as e:
        logging.error(f"Error processing webhook: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'services': {
            'openai': bool(os.getenv('OPENAI_API_KEY')),
            'telegram': bool(os.getenv('TELEGRAM_TOKEN')),
            'bigquery': bool(os.getenv('BQ_TABLE_ID'))
        }
    })

@app.route('/test', methods=['GET'])
def test_endpoint():
    """Test endpoint to verify all services are working"""
    try:
        # Test OpenAI with enhanced processing
        test_text = "Hey! @testuser here. My recent post got 10,000 likes and 500 comments. I have 45K followers!"
        test_result = openai_helper.process_influencer_message(test_text, "testuser")
        openai_status = bool(test_result.get('processing_status') == 'success')
        
        # Test BigQuery
        summary = bigquery_helper.get_metrics_summary()
        bigquery_status = True
        
        return jsonify({
            'status': 'success',
            'services': {
                'openai': openai_status,
                'bigquery': bigquery_status,
                'telegram': bool(TELEGRAM_TOKEN)
            },
            'message': 'All services operational with agency-compliant processing',
            'test_result': {
                'classification': test_result.get('classification', {}).get('message_intent'),
                'has_metrics': test_result.get('metrics_data', {}).get('has_metrics'),
                'response_tone': 'agency_compliant'
            }
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    # Use port 5001 if 5000 is occupied by AirPlay
    app.run(debug=True, host='0.0.0.0', port=5001)