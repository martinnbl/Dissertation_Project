import os
import json
import re
import requests
import fitz  # PyMuPDF

# Initialize OpenAI key and webhook destination from environment
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DESTINATION_URL = os.getenv("DESTINATION_URL")

# Convert Google Drive shared URL to direct download
def convert_google_drive_url(shared_url):
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', shared_url)
    if match:
        file_id = match.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return shared_url

# Download file from Google Drive with proper error handling
def download_google_drive_file(url, local_path):
    """Download file from Google Drive, handling potential redirects and virus warnings"""
    session = requests.Session()
    
    # First request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response = session.get(url, headers=headers, stream=True, timeout=30)
    
    # Check if we got a virus warning page (Google Drive sometimes shows this for large files)
    if 'virus' in response.text.lower() or 'download_warning' in response.url:
        # Extract the actual download URL
        for line in response.text.split('\n'):
            if 'download_warning' in line and 'href=' in line:
                import re
                match = re.search(r'href="([^"]*download_warning[^"]*)"', line)
                if match:
                    download_url = match.group(1).replace('&amp;', '&')
                    response = session.get('https://drive.google.com' + download_url, headers=headers, stream=True, timeout=30)
                    break
    
    response.raise_for_status()
    
    # Write the file
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    
    return len(response.content) if hasattr(response, 'content') else os.path.getsize(local_path)

# Extract text from PDF with better error handling
def extract_text_from_file(file_path):
    if not file_path.endswith(".pdf"):
        raise ValueError("Unsupported file type. Only PDF files are supported.")
    
    # Check if file exists and has content
    if not os.path.exists(file_path):
        raise ValueError(f"File does not exist: {file_path}")
    
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise ValueError("Downloaded file is empty")
    
    print(f"File exists, size: {file_size} bytes")
    
    # Try to detect if it's actually a PDF
    try:
        with open(file_path, 'rb') as f:
            header = f.read(4)
            if header != b'%PDF':
                # Check if it's HTML (Google Drive error page)
                f.seek(0)
                content = f.read(1000).decode('utf-8', errors='ignore')
                if '<html' in content.lower():
                    raise ValueError("Downloaded file appears to be an HTML page, not a PDF. Check Google Drive permissions.")
                else:
                    raise ValueError(f"File doesn't appear to be a PDF. Header: {header}")
    except Exception as e:
        raise ValueError(f"Error checking file format: {str(e)}")
    
    # Extract text using PyMuPDF
    text = ""
    try:
        doc = fitz.open(file_path)
        print(f"PDF opened successfully, {len(doc)} pages")
        
        for page_num, page in enumerate(doc):
            page_text = page.get_text()
            if page_text.strip():
                text += page_text + "\n"
                print(f"Page {page_num + 1}: {len(page_text)} characters extracted")
        
        doc.close()
        print(f"Total text extracted: {len(text)} characters")
        
    except Exception as e:
        raise ValueError(f"Error extracting PDF text: {str(e)}")
    
    if not text.strip():
        raise ValueError("No text could be extracted from the PDF")
    
    return text

# Extract JSON from unstructured response using regex
def extract_json_block(text):
    try:
        json_match = re.search(r'(\{[\s\S]+\})', text)
        if json_match:
            return json.loads(json_match.group(1))
    except Exception:
        pass
    return {"error": "Could not extract valid JSON from response", "raw_response": text}

# Call OpenAI to parse contract
def parse_contract_with_openai(contract_text):
    base_prompt = """
You are a contract parsing assistant. Read the following contract text and extract the following fields into a structured JSON object.

### Fields to extract:
- agency_name
- agency_address
- client_name
- client_address
- contract_date (format: YYYY-MM-DD)
- total_fee (numeric value only)
- currency
- promoted_service_product
- platforms (list of platforms mentioned)
- platform_1
- platform_1_number
- platform_2
- platform_2_number
- schedule (a list of content scheduled, see format below)
- post_duration (number of days the content should remain online)
- payment_deadline (date or period by which payment should be completed)
- agency_sign_date (format: YYYY-MM-DD)
- influencer_sign_date (format: YYYY-MM-DD)

### Schedule Format:
Each item in `schedule` should follow this structure:

{
  "schedule": [
    {
      "Platform": "Instagram",
      "Date": "YYYY-MM-DD",
      "Content Theme": "Brief description of the post theme",
      "Impressions": 0,
      "Views": 0,
      "Likes": 0,
      "Comments": 0,
      "Shares": 0
    }
  ]
}

Only return the final JSON object — no explanations, no extra text. If any field is missing or unclear in the contract, return null or an empty string for that field.
"""
    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "gpt-4",
            "messages": [
                {"role": "system", "content": "You are a contract parsing assistant."},
                {"role": "user", "content": base_prompt + "\n\n" + contract_text}
            ],
            "temperature": 0.1,
            "max_tokens": 2000
        }
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        response_data = response.json()
        content = response_data["choices"][0]["message"]["content"]

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return extract_json_block(content)

    except Exception as e:
        return {"error": f"OpenAI API error: {str(e)}"}

# Lambda Handler
def lambda_handler(event, context):
    print("=== Lambda function started ===")
    print(f"Received event: {json.dumps(event)}")

    try:
        body = json.loads(event.get('body', '{}'))
        print(f"Parsed body: {json.dumps(body)}")
    except Exception as e:
        print(f"Error parsing body: {str(e)}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON body"})
        }

    # Handle both 'new_files' (array) and 'latest_file' (single file) formats
    files_to_process = []
    
    if 'new_files' in body:
        files_to_process = body.get('new_files', [])
    elif 'latest_file' in body:
        # Convert single file to array format
        latest_file = body.get('latest_file', {})
        if latest_file.get('name'):
            files_to_process = [latest_file]
    
    if not files_to_process:
        print("No files to process found in request")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No files found in request. Expected 'new_files' array or 'latest_file' object."})
        }

    print(f"Processing {len(files_to_process)} file(s)")
    results = []

    for file in files_to_process:
        name = file.get('name')
        
        # Check if file content is provided directly (base64 encoded)
        if 'content' in file:
            print(f"Processing file with direct content: {name}")
            local_path = f"/tmp/{name}"
            
            try:
                import base64
                file_content = base64.b64decode(file['content'])
                with open(local_path, 'wb') as f:
                    f.write(file_content)
                print(f"File written from base64 content, size: {len(file_content)} bytes")
            except Exception as e:
                print(f"Failed to decode base64 content for {name}: {str(e)}")
                results.append({"file": name, "error": f"Failed to decode base64 content: {str(e)}"})
                continue
                
        elif 'url' in file:
            print(f"Processing file with URL: {name}")
            shared_url = file.get('url')
            if not shared_url:
                results.append({"file": name or "unknown", "error": "Missing URL"})
                continue

            url = convert_google_drive_url(shared_url)
            local_path = f"/tmp/{name}"

            try:
                print(f"Downloading file from: {url}")
                file_size = download_google_drive_file(url, local_path)
                print(f"File downloaded successfully, size: {file_size} bytes")
            except Exception as e:
                print(f"Download failed for {name}: {str(e)}")
                results.append({"file": name, "error": f"Download failed: {str(e)}"})
                continue
        else:
            results.append({"file": name or "unknown", "error": "Missing content or URL"})
            continue

        try:
            print(f"Extracting text from: {local_path}")
            contract_text = extract_text_from_file(local_path)
            if not contract_text.strip():
                results.append({"file": name, "error": "No text extracted from file"})
                continue
            print(f"Text extracted successfully, length: {len(contract_text)} characters")
        except Exception as e:
            print(f"Text extraction failed for {name}: {str(e)}")
            results.append({"file": name, "error": f"Text extraction failed: {str(e)}"})
            continue
        finally:
            try:
                os.remove(local_path)
            except:
                pass

        print("Sending text to OpenAI for parsing...")
        parsed_json = parse_contract_with_openai(contract_text)

        print("=== PARSED JSON START ===")
        print(json.dumps(parsed_json, indent=2))
        print("=== PARSED JSON END ===")

        result_entry = {"file": name, "parsed": parsed_json}
        results.append(result_entry)

        if DESTINATION_URL:
            try:
                print(f"Sending parsed data to destination: {DESTINATION_URL}")
                post_resp = requests.post(DESTINATION_URL, json=parsed_json, timeout=30)
                result_entry["post_status"] = post_resp.status_code
                result_entry["post_response"] = post_resp.text
                print(f"Post response: {post_resp.status_code}")
            except Exception as e:
                print(f"Post to destination failed: {str(e)}")
                result_entry["post_error"] = str(e)

    print(f"Final results: {json.dumps(results, indent=2)}")
    print("=== Lambda function completed ===")

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "processed", "results": results}),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
    }