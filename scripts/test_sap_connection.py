import os
import requests
from dotenv import load_dotenv

def test_sap():
    # 1. Unlock the vault (.env file)
    load_dotenv()

    # 2. Retrieve the keys
    api_key = os.getenv("SAP_API_KEY")
    base_url = os.getenv("SAP_SANDBOX_BASE_URL")

    # 3. Construct the final Endpoint (Appending /SalesOrder to the base)
    target_url = f"{base_url}/SalesOrder?$top=1" # We use $top=1 to just grab 1 order

    # 4. SAP Sandbox requires the API key to be passed in the Headers
    headers = {
        "APIKey": api_key,
        "Accept": "application/json"
    }

    print(f"Pinging SAP ES5 via Sandbox: {target_url}...")
    
    # 5. Fire the request!
    response = requests.get(target_url, headers=headers)

    # 6. Check the result
    if response.status_code == 200:
        print("\n✅ CONNECTION SUCCESSFUL! We have extracted Enterprise Data.")
        print("-" * 50)
        data = response.json()
        print(data['value'][0]) # Print the first sales order dictionary
    else:
        print(f"\n❌ CONNECTION FAILED! Status Code: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    test_sap()