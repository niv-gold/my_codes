import requests
import json
from datetime import datetime

# Step 1. Define API endpoint and key
API_KEY = "6b5c24c50e054d60abf20a9f63211f7f"  # Replace with your real key
URL = f"https://api.currencyfreaks.com/latest?apikey={API_KEY}&symbols=USD,CAD,CHF,EUR,GBP,ILS"

# Step 2. Send GET request
response = requests.get(URL)

# Step 3. Check and process response
if response.status_code == 200:
    raw_data = response.json()

    # Step 4. Embed and reshape data to your required structure
    result = {
        "base": raw_data.get("base", "USD"),
        "date": raw_data.get("date", datetime.utcnow().strftime("%Y-%m-%d")),
        "rates": raw_data.get("rates", {}),
        "timestamp": datetime.utcnow().isoformat()
    }

    # Step 5. Print result nicely
    print(json.dumps(result, indent=2))

    # Optional: Write to file for Spark Structured Streaming
    with open("/tmp/fx_rate_snapshot.json", "a") as f:
        f.write(json.dumps(result) + "\n")

else:
    print(f"Request failed: {response.status_code} - {response.text}")
