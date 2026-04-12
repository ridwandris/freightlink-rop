import os
import requests
import json
from dotenv import load_dotenv

def test_real_freight_api():
    load_dotenv()
    api_key = os.getenv("TERMINAL49_API_KEY")
    
    # Terminal49 requires the token to be prefixed in the header
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/vnd.api+json"
    }

    # We use their official sandbox tracking number for testing
    test_tracking_number = "TEST-TR-SUCCEEDED"
    
    # Terminal49 API Endpoint for tracking requests
    url = f"https://api.terminal49.com/v2/tracking_requests?include=containers"

    print(f"🚢 Pinging Terminal49 Production API...")
    
   # We send a POST request to track a specific bill of lading
    payload = {
      "data": {
        "type": "tracking_request",
        "attributes": {
          "request_number": "TEST-TR-SUCCEEDED",
          "request_type": "bill_of_lading",
          "scac": "TEST"  # Required for sandbox test numbers
        }
      }
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in [200, 201]:
        print("\n✅ ENTERPRISE FREIGHT DATA EXTRACTED SUCCESSFULLY!")
        print("-" * 50)
        data = response.json()
        
        # Terminal49 returns massive JSONAPI structures. Let's just print a snippet.
        print(json.dumps(data, indent=4))
        print("-" * 50)
        print("Notice the detailed status codes, ETAs, and routing information!")
        
    elif response.status_code == 401:
        print("\n❌ AUTH FAILED: Check your Terminal49 API Key.")
    else:
        print(f"\n❌ FAILED! Status Code: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    test_real_freight_api()