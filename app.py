from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import time
from urllib.request import Request, urlopen
import pandas as pd
import base64
from postmarker.core import PostmarkClient
import os
import threading
import openpyxl
import requests

app = Flask(__name__)
CORS(app)  

MY_API_KEY = os.environ.get("MY_API_KEY")
FM_TOKEN = os.environ.get("FM_TOKEN")
VERDI_URL = os.environ.get("VERDI_URL")
API_URL = "https://api.tookanapp.com/v2/get_fare_estimate"

POSTMARK_TOKEN = os.environ.get("POSTMARK_TOKEN")
senderEmail = "Support@tryverdi.com"
recipientEmail = "mrharoonkhan11@gmail.com"


#!------------------------------------------------------------------------------------------------------------------------------------------------------------------
#* --------------------------------------------FARE from 1 BRANCH to ALL AREAS of Kuwait----------------------------------------------------------------------------


def getFareData(pickup_lat, pickup_lng, template):

    # Load the JSON file
    with open('AreasWithBlocks.json', 'r') as file:
        areas = json.load(file)

    results = []

    for area in areas:
        nhood_name = area['area_name']
        delivery_longitude = str(area['lng'])
        delivery_latitude = str(area['lat'])
        block_number = str(area["name_en"])

        # Prepare API request data
        data = {
            "template_name": template,
            "pickup_longitude": pickup_lng,
            "pickup_latitude": pickup_lat,
            "api_key": MY_API_KEY,
            "delivery_latitude": delivery_latitude,
            "delivery_longitude": delivery_longitude,
            "formula_type": 2,
            "map_keys": {
                "fm_token": FM_TOKEN
            },
            "map_type": 0
        }

        # Convert the dictionary to a JSON-formatted byte string
        json_data = json.dumps(data).encode('utf-8')

        headers = {
            'Content-Type': 'application/json'
        }

        try:
            # Make the API request
            request = Request(API_URL, data=json_data, headers=headers)
            response_body = urlopen(request).read()
            response_json = json.loads(response_body.decode('utf-8'))

            # Check if the response is successful
            if response_json.get('status') == 200:
                estimated_fare = response_json['data']['estimated_fare']
                distance = response_json['data']['distance']
                # Append results
                results.append({
                    'Area': nhood_name,
                    'Block': block_number,
                    'Estimated Fare': estimated_fare,
                    'Distance (meters)': distance
                })
                print(f"Processed {nhood_name}: Fare = {estimated_fare}, Distance = {distance} meters")
            else:
                print(f"Error for {nhood_name}: {response_json.get('message')}")
                results.append({
                    'Area': nhood_name,
                    'Block': block_number,
                    'Estimated Fare': None,
                    'Distance (meters)': None
                })

        except Exception as e:
            print(f"Exception for {nhood_name}: {str(e)}")
            results.append({
                'Areas': nhood_name,
                'Estimated Fare': None,
                'Distance (meters)': None
            })

        # Add a 5-second delay between API calls
        # time.sleep(5)

    return results

def sendEmail(results, clientName):
    try:
        # Convert results to a pandas DataFrame
        df = pd.DataFrame(results)
        
        # Save DataFrame to Excel
        excel_file = f"{clientName} fare estimate.xlsx"
        df.to_excel(excel_file, index=False)
        
        # Read the file and encode in base64
        with open(excel_file, "rb") as f:
            excel_data = f.read()
            encoded_excel = base64.b64encode(excel_data).decode()

        # Send email using Postmark
        client = PostmarkClient(server_token=POSTMARK_TOKEN)
        client.emails.send(
            From=senderEmail,
            To=recipientEmail,
            Subject="Area wise fare estimate",
            HtmlBody="<strong>Please find the attached Excel file.</strong>",
            Attachments=[
                {
                    "Name": f"{clientName} fare estimate.xlsx",
                    "Content": encoded_excel,
                    "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                }
            ]
        )

    except Exception as e:
        print(f"Error sending email: {str(e)}")

def fareAndEmail(pickup_lat, pickup_lng, template, clientName):
    results = getFareData(pickup_lat, pickup_lng, template)
    sendEmail(results, clientName)

# TODO: Endpoint for getting fares from the branch to all areas of Kuwait
@app.route('/get_all_fare', methods=['POST'])
def get_all_fare():
    data = request.get_json()

    pickup_lat = data.get('pickup_lat')
    pickup_lng = data.get('pickup_lng')
    template = data.get('template')
    clientName = data.get('clientName')

    # Start the heavy work in a new thread
    thread = threading.Thread(target=fareAndEmail, args=(pickup_lat, pickup_lng, template, clientName))
    thread.start()

    return jsonify({
        "message": "Yooooo congrats bro your micro service is actually working 😂"
    })


# !-----------------------------------------------------------------------------------------------------------------------------------------------------------------
#* -----------------------------------------NUMBER of ORDERS / HOUR (ALL or SPECIFIC CLIENT)------------------------------------------------------------------------


def getHourlyOrders(start_date, end_date, filter_by):
    apiURL = f"{VERDI_URL}user_id={filter_by}&start_date={start_date}&end_date={end_date}"
    response = requests.get(url=apiURL)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return f"Error: {response.status_code}, {response.text}"

# TODO: Endpoint for getting NUMBER OF ORDERS PER HOUR (Specific client or all clients)
@app.route('/get_hourly_orders', methods=["POST"])
def get_hourly_orders():

    # data = request.get_json()
    # start_date = data.get('start_date')
    # end_date = data.get('end_date')
    # filter_by = data.get('filter_by')
    start_date = "2025-04-01"
    end_date = "2025-04-02"
    filter_by = "all"
    data = getHourlyOrders(start_date, end_date, filter_by)

    print(data)


# !-----------------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    app.run(debug=False)
