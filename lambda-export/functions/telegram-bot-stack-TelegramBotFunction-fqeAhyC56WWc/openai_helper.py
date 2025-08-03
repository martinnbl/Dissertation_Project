import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime
import re

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

class OpenAIHelper:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.logger = logging.getLogger(__name__)
        
        # Agency tone guidelines
        self.agency_tone_guidelines = """
        TONE OF VOICE GUIDELINES:
        - Professional, supportive, confident, modern, and personable
        - Clear, warm, and professional with a little personality
        - Use friendly and efficient language
        - Keep messages short and skimmable
        - Show appreciation and respect for influencer's time and craft
        - Use one emoji per message max to add tone (not fluff)
        - Never be robotic, overly formal, or use passive-aggressive tone
        - Sound like someone who values their craft and knows how to run a smooth show
        """
    
    def classify_message_intent(self, message_text, username):
        """
        Classify the intent of an influencer's message to determine appropriate response
        
        Args:
            message_text (str): Influencer's message
            username (str): Instagram username
            
        Returns:
            dict: Message classification and suggested response approach
        """
        try:
            prompt = f"""Analyze this message from influencer @{username} and classify the intent:

Message: "{message_text}"

{self.agency_tone_guidelines}

Classify the message intent and suggest response approach. Return JSON:
{{
    "message_intent": "metrics_provided|metrics_unavailable|question|confirmation|delay_notification|payment_inquiry|general_chat|brief_response",
    "sentiment": "positive|neutral|negative|frustrated",
    "contains_metrics": true/false,
    "requires_immediate_response": true/false,
    "suggested_response_type": "acknowledge_metrics|request_clarification|provide_support|escalate_to_human|send_reminder",
    "key_points": ["list of main points from message"],
    "urgency_level": "low|medium|high",
    "follow_up_needed": true/false,
    "follow_up_timeline": "within_hour|same_day|next_business_day|none"
}}

Examples:
- Metrics sharing: "metrics_provided" + "acknowledge_metrics"
- "I can't access my analytics": "metrics_unavailable" + "provide_support"
- Questions about brief: "question" + "provide_support"
- Payment concerns: "payment_inquiry" + "escalate_to_human"
- Running late: "delay_notification" + "provide_support"

Return only the JSON:"""

            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing influencer communications for a creative agency. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            classification = json.loads(response.choices[0].message.content)
            self.logger.info(f"Message classified as: {classification.get('message_intent')} for @{username}")
            
            return classification
            
        except Exception as e:
            self.logger.error(f"Error classifying message intent: {e}")
            return {
                "message_intent": "general_chat",
                "sentiment": "neutral",
                "contains_metrics": False,
                "requires_immediate_response": False,
                "suggested_response_type": "provide_support",
                "urgency_level": "low"
            }
    
    def generate_contextual_response(self, message_text, username, classification, campaign_context=None):
        """
        Generate appropriate response based on message classification and agency guidelines
        
        Args:
            message_text (str): Original influencer message
            username (str): Instagram username
            classification (dict): Message classification from classify_message_intent
            campaign_context (dict): Optional campaign context (deadlines, status, etc.)
            
        Returns:
            dict: Generated response with metadata
        """
        try:
            # Build context for response generation
            context = f"Campaign context: {campaign_context}" if campaign_context else "No specific campaign context provided"
            
            prompt = f"""Generate a response to this influencer message following our agency tone guidelines:

INFLUENCER: @{username}
MESSAGE: "{message_text}"
CLASSIFICATION: {json.dumps(classification, indent=2)}
CONTEXT: {context}

{self.agency_tone_guidelines}

RESPONSE TEMPLATES BY INTENT:

METRICS_PROVIDED:
- "Everything's in — thank you so much 🙏 We'll review and get back to you with final approvals or feedback by [day]."
- "Perfect! Thanks for sharing these 😊 We'll analyze the performance and keep you posted on next steps."

METRICS_UNAVAILABLE:
- "No worries at all! Let us know when you can access them — we're here to help if you run into any issues 😊"
- "Thanks for letting us know! No rush — send them over when convenient."

QUESTION/SUPPORT:
- "Great question! [answer] Let us know if you need anything else — we've got you."
- "Happy to clarify! [explanation] Always feel free to reach out if something's unclear."

DELAY_NOTIFICATION:
- "Thanks for the heads up! No stress — let us know if you need any support to get things sorted 😊"
- "Appreciate you letting us know! We'll adjust timelines accordingly."

Generate a response that:
1. Matches our tone (professional, warm, personable)
2. Addresses their specific message
3. Shows appreciation for their communication
4. Provides clear next steps if needed
5. Uses max one emoji to add warmth
6. Keeps it concise and skimmable

Return JSON:
{{
    "response_message": "the actual response text",
    "response_type": "acknowledgment|support|clarification|escalation",
    "includes_next_steps": true/false,
    "follow_up_required": true/false,
    "follow_up_date": "YYYY-MM-DD or null",
    "escalation_needed": true/false,
    "internal_notes": "notes for team about this interaction"
}}

Return only the JSON:"""

            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a skilled account manager at a creative agency, expert at maintaining warm professional relationships with influencers."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=800
            )
            
            response_data = json.loads(response.choices[0].message.content)
            self.logger.info(f"Generated response for @{username}: {response_data.get('response_type')}")
            
            return response_data
            
        except Exception as e:
            self.logger.error(f"Error generating contextual response: {e}")
            return {
                "response_message": "Thanks for your message! We'll get back to you shortly 😊",
                "response_type": "acknowledgment",
                "includes_next_steps": False,
                "follow_up_required": True,
                "escalation_needed": True,
                "internal_notes": f"Error generating response - manual review needed: {str(e)}"
            }
    
    def extract_metrics_from_text(self, text, username):
        """
        Enhanced metrics extraction with better context awareness and validation
        
        Args:
            text (str): Influencer's response with metrics data
            username (str): Instagram username
            
        Returns:
            dict: Extracted metrics data with post-level information and quality score
        """
        try:
            # First, check if this actually contains metrics
            contains_metrics = self._quick_metrics_check(text)
            
            if not contains_metrics:
                return self._create_empty_metrics_response_with_username(username)
            
            # FIRST: Try to extract username from the text itself if username is "unknown"
            if username == "unknown":
                extracted_username = extract_influencer_name(text)
                if extracted_username != "unknown":
                    username = extracted_username
                    self.logger.info(f"Updated username from text extraction: {username}")
            
            # Enhanced schema with quality indicators
            schema = {
                "has_metrics": "boolean - true if metrics found",
                "influencer_name": "string - Instagram username without @",
                "data_quality_score": "float - 0-1 score indicating completeness/reliability",
                "extraction_confidence": "float - 0-1 confidence in extraction accuracy",
                "recent_posts": [
                    {
                        "post_id": "string - post ID or URL",
                        "url": "string - post URL or description", 
                        "media_type": "string - photo|video|reel|carousel",
                        "likes": "integer - number of likes",
                        "comments": "integer - number of comments", 
                        "post_date": "string - YYYY-MM-DD format",
                        "views": "integer - views (for videos/reels) or null",
                        "reach": "integer - reach if available or null",
                        "impressions": "integer - impressions if available or null",
                        "shares": "integer - shares if available or null",
                        "completeness_score": "float - 0-1 indicating how complete this post data is"
                    }
                ],
                "followers_count": "integer - total followers or null",
                "following_count": "integer - total following or null", 
                "posts_count": "integer - total posts or null",
                "avg_likes_per_post": "integer - average likes or null",
                "avg_comments_per_post": "integer - average comments or null",
                "engagement_rate": "float - engagement rate percentage or null",
                "missing_data_points": "array - list of metrics mentioned but not extractable",
                "data_source_reliability": "string - high|medium|low based on how data was presented"
            }
            
            schema_json = json.dumps(schema, indent=2)
            
            prompt = f"""Extract Instagram metrics from this influencer response with enhanced quality assessment:

INFLUENCER: @{username}
MESSAGE: "{text}"

{self.agency_tone_guidelines}

Expected Output Schema:
{schema_json}

ENHANCED EXTRACTION INSTRUCTIONS:
1. Set influencer_name to: "{username}" (without @ symbol)
2. Look for individual post performance data (likes, comments, views, etc.)
3. Extract post URLs like "https://www.instagram.com/p/DKWmrCONQqs/" 
4. QUALITY ASSESSMENT: Rate data completeness and reliability
5. HANDLE INFORMAL LANGUAGE: "got like 2k likes" = 2000 likes
6. ABBREVIATIONS: "45K" = 45000, "1.2M" = 1200000
7. TYPOS: "liks" = likes, "coments" = comments
8. If metrics are found for ANY posts, set has_metrics to true
9. Rate extraction confidence based on clarity of data provided
10. Note any missing data points that were mentioned but unclear

QUALITY SCORING:
- data_quality_score: 1.0 = complete metrics, 0.5 = partial, 0.1 = minimal
- extraction_confidence: 1.0 = very clear data, 0.5 = some interpretation needed
- completeness_score (per post): 1.0 = all fields filled, 0.5 = basic metrics only

HANDLE INFORMAL RESPONSES:
- "my post got like 2,341 liks and 156 coments" → likes: 2341, comments: 156
- "around 67k followers" → followers_count: 67000
- "posted 3 days ago" → calculate approximate date

Return ONLY the JSON object:"""
            
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a data extraction expert specializing in social media metrics from informal influencer communications. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            # Parse and validate the response
            extracted_data = json.loads(response.choices[0].message.content)
            
            # Ensure influencer_name is set correctly
            if not extracted_data.get('influencer_name') or extracted_data.get('influencer_name') == 'unknown':
                extracted_data['influencer_name'] = username
            
            # Enhanced validation and cleaning
            extracted_data = self._validate_and_clean_metrics_enhanced(extracted_data, username)
            
            self.logger.info(f"Successfully extracted metrics for @{username} with quality score: {extracted_data.get('data_quality_score', 0)}")
            return extracted_data
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse OpenAI response as JSON: {e}")
            return self._create_empty_metrics_response_with_username(username)
        except Exception as e:
            self.logger.error(f"Error in extract_metrics_from_text: {e}")
            return self._create_empty_metrics_response_with_username(username)
    
    def _quick_metrics_check(self, text):
        """Quick check if text likely contains metrics data"""
        metrics_indicators = [
            'likes', 'comments', 'followers', 'views', 'reach', 'impressions',
            'engagement', 'post', 'instagram.com/p/', 'analytics', 'stats',
            r'\d+[kKmM]?\s*(likes|comments|followers|views)',  # Numbers + metrics
            r'\d{1,3}[,\s]*\d{3}',  # Formatted numbers like 1,250
        ]
        
        text_lower = text.lower()
        for indicator in metrics_indicators:
            if isinstance(indicator, str):
                if indicator in text_lower:
                    return True
            else:
                if re.search(indicator, text_lower):
                    return True
        
        return False
    
    def _validate_and_clean_metrics_enhanced(self, data, username):
        """Enhanced validation with quality scoring"""
        try:
            # Ensure has_metrics is boolean
            data['has_metrics'] = bool(data.get('has_metrics', False))
            
            # Ensure influencer_name is set
            if not data.get('influencer_name') or data.get('influencer_name') == 'unknown':
                data['influencer_name'] = username
            
            # Quality scoring defaults
            data['data_quality_score'] = data.get('data_quality_score', 0.5)
            data['extraction_confidence'] = data.get('extraction_confidence', 0.5)
            data['missing_data_points'] = data.get('missing_data_points', [])
            data['data_source_reliability'] = data.get('data_source_reliability', 'medium')
            
            # Validate recent_posts with enhanced cleaning
            if 'recent_posts' in data and data['recent_posts']:
                cleaned_posts = []
                for i, post in enumerate(data['recent_posts']):
                    # Extract post_id from URL if not provided
                    post_id = post.get('post_id')
                    post_url = post.get('url', '')
                    
                    if not post_id and post_url:
                        # Extract post ID from Instagram URL
                        id_match = re.search(r'/p/([A-Za-z0-9_-]+)/', post_url)
                        if id_match:
                            post_id = id_match.group(1)
                    
                    cleaned_post = {
                        'post_id': post_id or f"post_{i+1}",
                        'url': post_url or f"https://instagram.com/{username}/p/post_{i+1}",
                        'media_type': self._validate_media_type(post.get('media_type')),
                        'likes': self._safe_int_enhanced(post.get('likes')),
                        'comments': self._safe_int_enhanced(post.get('comments')),
                        'post_date': self._validate_date(post.get('post_date')),
                        'views': self._safe_int_enhanced(post.get('views')),
                        'reach': self._safe_int_enhanced(post.get('reach')),
                        'impressions': self._safe_int_enhanced(post.get('impressions')),
                        'shares': self._safe_int_enhanced(post.get('shares')),
                        'completeness_score': post.get('completeness_score', 0.5)
                    }
                    cleaned_posts.append(cleaned_post)
                data['recent_posts'] = cleaned_posts
            else:
                data['recent_posts'] = []
            
            # Validate numeric fields with enhanced parsing
            data['followers_count'] = self._safe_int_enhanced(data.get('followers_count'))
            data['following_count'] = self._safe_int_enhanced(data.get('following_count'))
            data['posts_count'] = self._safe_int_enhanced(data.get('posts_count'))
            data['avg_likes_per_post'] = self._safe_int_enhanced(data.get('avg_likes_per_post'))
            data['avg_comments_per_post'] = self._safe_int_enhanced(data.get('avg_comments_per_post'))
            data['engagement_rate'] = self._safe_float(data.get('engagement_rate'))
            
            # Calculate missing averages and engagement rate
            if data['recent_posts'] and not data['avg_likes_per_post']:
                likes = [p['likes'] for p in data['recent_posts'] if p['likes'] is not None]
                if likes:
                    data['avg_likes_per_post'] = int(sum(likes) / len(likes))
            
            if data['recent_posts'] and not data['avg_comments_per_post']:
                comments = [p['comments'] for p in data['recent_posts'] if p['comments'] is not None]
                if comments:
                    data['avg_comments_per_post'] = int(sum(comments) / len(comments))
            
            # Calculate engagement rate if possible
            if (data['avg_likes_per_post'] and data['avg_comments_per_post'] and 
                data['followers_count'] and not data['engagement_rate']):
                engagement = ((data['avg_likes_per_post'] + data['avg_comments_per_post']) / 
                             data['followers_count']) * 100
                data['engagement_rate'] = round(engagement, 2)
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error validating metrics: {e}")
            return data
    
    def _safe_int_enhanced(self, value):
        """Enhanced integer parsing for social media numbers"""
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                # Handle abbreviations first
                value = value.lower().strip()
                
                # Handle K (thousands) and M (millions)
                if value.endswith('k'):
                    base = float(value[:-1])
                    return int(base * 1000)
                elif value.endswith('m'):
                    base = float(value[:-1])
                    return int(base * 1000000)
                
                # Remove commas, spaces, and other non-numeric characters
                clean_value = re.sub(r'[^\d.]', '', value)
                
                # Handle decimal points (like 1.5K that was already processed)
                if '.' in clean_value:
                    return int(float(clean_value))
                
                return int(clean_value) if clean_value else None
            
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def process_influencer_message(self, message_text, username, campaign_context=None):
        """
        Main processing function that handles the full influencer message workflow
        
        Args:
            message_text (str): Influencer's message
            username (str): Instagram username  
            campaign_context (dict): Optional campaign context
            
        Returns:
            dict: Complete processing result with classification, metrics, and response
        """
        try:
            # Step 1: Classify the message intent
            classification = self.classify_message_intent(message_text, username)
            
            # Step 2: Extract metrics if present
            metrics_data = None
            if classification.get('contains_metrics', False):
                metrics_data = self.extract_metrics_from_text(message_text, username)
            
            # Step 3: Generate appropriate response
            response_data = self.generate_contextual_response(
                message_text, username, classification, campaign_context
            )
            
            # Step 4: Compile complete result
            result = {
                "timestamp": datetime.now().isoformat(),
                "influencer_username": username,
                "original_message": message_text,
                "classification": classification,
                "metrics_data": metrics_data,
                "response": response_data,
                "processing_status": "success"
            }
            
            self.logger.info(f"Successfully processed message from @{username}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing influencer message: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "influencer_username": username,
                "original_message": message_text,
                "processing_status": "error",
                "error_message": str(e),
                "response": {
                    "response_message": "Thanks for your message! We'll get back to you shortly 😊",
                    "escalation_needed": True,
                    "internal_notes": f"Processing error - manual review required: {str(e)}"
                }
            }
    
    # Keep existing helper methods with some improvements
    def _validate_media_type(self, media_type):
        """Validate and normalize media type"""
        if not media_type:
            return 'photo'
        
        media_type = media_type.lower()
        valid_types = ['photo', 'video', 'reel', 'carousel']
        
        # Map common variations
        type_mapping = {
            'image': 'photo', 'pic': 'photo', 'picture': 'photo',
            'vid': 'video', 'clip': 'video', 
            'story': 'reel', 'stories': 'reel',
            'slide': 'carousel', 'slides': 'carousel', 'swipe': 'carousel'
        }
        
        if media_type in valid_types:
            return media_type
        elif media_type in type_mapping:
            return type_mapping[media_type]
        else:
            return 'photo'  # default
    
    def _validate_date(self, date_str):
        """Enhanced date validation with relative date handling"""
        if not date_str:
            return datetime.now().strftime('%Y-%m-%d')
        
        try:
            # Handle relative dates like "3 days ago", "yesterday"
            if 'ago' in date_str.lower() or 'yesterday' in date_str.lower():
                from datetime import timedelta
                today = datetime.now()
                
                if 'yesterday' in date_str.lower():
                    return (today - timedelta(days=1)).strftime('%Y-%m-%d')
                elif 'day' in date_str.lower():
                    days_match = re.search(r'(\d+)', date_str)
                    if days_match:
                        days = int(days_match.group(1))
                        return (today - timedelta(days=days)).strftime('%Y-%m-%d')
                elif 'week' in date_str.lower():
                    weeks_match = re.search(r'(\d+)', date_str)
                    if weeks_match:
                        weeks = int(weeks_match.group(1))
                        return (today - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
            
            # Try standard date formats
            date_patterns = [
                '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
                '%Y-%m-%d %H:%M:%S', '%d-%m-%Y'
            ]
            
            for pattern in date_patterns:
                try:
                    parsed_date = datetime.strptime(date_str, pattern)
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            return datetime.now().strftime('%Y-%m-%d')
            
        except Exception:
            return datetime.now().strftime('%Y-%m-%d')
    
    def _safe_float(self, value):
        """Safely convert value to float"""
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                clean_value = value.replace('%', '').replace(',', '').strip()
                return float(clean_value) if clean_value else None
            
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _create_empty_metrics_response_with_username(self, username):
        """Create empty metrics response with quality indicators"""
        return {
            "has_metrics": False,
            "influencer_name": username,
            "data_quality_score": 0.0,
            "extraction_confidence": 1.0,  # High confidence in "no metrics"
            "recent_posts": [],
            "followers_count": None,
            "following_count": None,
            "posts_count": None,
            "avg_likes_per_post": None,
            "avg_comments_per_post": None,
            "engagement_rate": None,
            "missing_data_points": [],
            "data_source_reliability": "high"  # High reliability in determining no metrics
        }


# Enhanced standalone functions
def extract_influencer_name(message):
    """Enhanced influencer name extraction"""
    try:
        # Comprehensive username patterns
        username_patterns = [
            r'@([a-zA-Z0-9._]+)',
            r'username is @([a-zA-Z0-9._]+)',
            r'my username is @([a-zA-Z0-9._]+)',
            r'handle is @([a-zA-Z0-9._]+)',
            r'my handle is @([a-zA-Z0-9._]+)',
            r'ig: @([a-zA-Z0-9._]+)',
            r'instagram: @([a-zA-Z0-9._]+)',
            r"i'm @([a-zA-Z0-9._]+)",
            r"it's @([a-zA-Z0-9._]+)",
        ]
        
        for pattern in username_patterns:
            username_match = re.search(pattern, message, re.IGNORECASE)
            if username_match:
                username = username_match.group(1)
                logging.info(f"Extracted username via regex: {username}")
                return username
        
        # Fallback to OpenAI with enhanced prompt
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": """Extract Instagram username from influencer messages. 

Look for patterns like @username, "my username is", "i'm @", etc.
Return ONLY the username without @ symbol.
If no username found, return "unknown"

Examples:
"Hey, my username is @fitnessguru" → "fitnessguru"
"@johndoe here!" → "johndoe"
"it's @wellness_coach" → "wellness_coach"
"no mention" → "unknown" """
                },
                {
                    "role": "user", 
                    "content": f"Extract username: {message}"
                }
            ],
            max_tokens=20,
            temperature=0
        )
        
        username = response.choices[0].message.content.strip().replace('@', '')
        logging.info(f"Extracted username via OpenAI: {username}")
        
        return username if username and username != "unknown" else "unknown"
        
    except Exception as e:
        logging.error(f"Error extracting username: {e}")
        return "unknown"


def structure_instagram_metrics(raw_data, influencer_name, request_date=None):
    """Legacy function - redirects to new enhanced method"""
    try:
        helper = OpenAIHelper()
        return helper.extract_metrics_from_text(raw_data, influencer_name)
    except Exception as e:
        logging.error(f"Error structuring metrics: {e}")
        return {
            "has_metrics": False,
            "influencer_name": influencer_name,
            "recent_posts": [],
            "followers_count": None,
            "following_count": None,
            "posts_count": None,
            "avg_likes_per_post": None,
            "avg_comments_per_post": None,
            "engagement_rate": None
        }


# New convenience function for full message processing
def process_influencer_telegram_message(message_text, username, campaign_context=None):
    """
    Main entry point for processing influencer messages in Telegram bot
    
    Args:
        message_text (str): The influencer's message
        username (str): Instagram username (can be "unknown")
        campaign_context (dict): Optional campaign information
        
    Returns:
        dict: Complete processing result ready for bot response
    """
    helper = OpenAIHelper()
    return helper.process_influencer_message(message_text, username, campaign_context)