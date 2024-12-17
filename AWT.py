# Databricks notebook source
pip install requests


# COMMAND ----------

import requests
import pandas as pd

# Redash API key and query result URL
api_key = 'your_api_key_here'
url = 'https://redash.razorpay.com/api/queries/11079/results.csv?api_key=e70yEWSFm5Dtve6vubiAlAAQW69FWwHQj3H60ih6'

# Fetch the data from Redash
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    
    # Print the raw data for inspection
    print("Raw data from API response:")
    print(data)
    
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    print("Response content:")
    print(response.text)


# COMMAND ----------

response

# COMMAND ----------


