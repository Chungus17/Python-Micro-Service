from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from urllib.request import Request, urlopen
import pandas as pd
import base64
from postmarker.core import PostmarkClient
import os
import threading
import requests
from collections import defaultdict
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime

app = Flask(__name__)
CORS(app)  

MY_API_KEY = os.environ.get("MY_API_KEY")
FM_TOKEN = os.environ.get("FM_TOKEN")
VERDI_URL = os.environ.get("VERDI_URL")
VERDI_API_KEY = os.environ.get("VERDI_API_KEY")
API_URL = "https://api.tookanapp.com/v2/get_fare_estimate"

POSTMARK_TOKEN = os.environ.get("POSTMARK_TOKEN")
senderEmail = "Support@tryverdi.com"
recipientEmail = "mrharoonkhan11@gmail.com"

def getData(start_date, end_date, filter_by, clientName, task_function):
    apiURL = f"https://tryverdi.com/api/transaction_data?user_id={filter_by}&start_date={start_date}&end_date={end_date}"

    headers = {
        "Authorization": f"Bearer {VERDI_API_KEY}"
    }

    response = requests.get(url=apiURL, headers=headers)

    if response.status_code == 200:
        try:
            data = response.json()
            if task_function == "hourly orders":
                get_Hourly_Orders(data, clientName)
            elif task_function == "average fare":
                get_Average_Fare(data, clientName)
            elif task_function == "number of orders":
                get_Number_Of_Orders(data, clientName)
            elif task_function == "total fare":
                get_Total_Fare(data, clientName)
            elif task_function == "amount ranges":
                get_Amount_Ranges(data, clientName)
            elif task_function == "pickup counts":
                get_Pickup_Counts_Per_Area(data, clientName)
            else:
                return jsonify({
                    "message": "Wrong function called 😂"
                })
        except ValueError as e:
            return("Failed to parse JSON. Raw response was:")
    else:
        return(f"Request failed with status code {response.status_code}")

def send_email(excel_file, subject, clientName):
    try:
        
        # Read the file and encode in base64
        with open(excel_file, "rb") as f:
            excel_data = f.read()
            encoded_excel = base64.b64encode(excel_data).decode()

        # Send email using Postmark
        client = PostmarkClient(server_token=POSTMARK_TOKEN)
        client.emails.send(
            From=senderEmail,
            To=recipientEmail,
            Subject=subject,
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




#!------------------------------------------------------------------------------------------------------------------------------------------------------------------
#*---------------------------------------------FARE from 1 BRANCH to ALL AREAS of Kuwait----------------------------------------------------------------------------


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
#* ---------------------------------------------------DATA ANALYSIS ENDPOINT----------------------------------------------------------------------------------------

# TODO: Endpoint for getting NUMBER OF ORDERS PER HOUR (Specific client or all clients)
@app.route('/data_analysis', methods=["POST"])
def data_analysis():

    params_data = request.get_json()
    start_date = params_data.get('start_date')
    end_date = params_data.get('end_date')
    filter_by = params_data.get('filter_by')
    clientName = params_data.get('clientName')
    task_function = params_data.get('task_function')

    thread = threading.Thread(target=getData, args=(start_date, end_date, filter_by, clientName, task_function))
    thread.start()

    return jsonify({
        "message": "Yooooo congrats bro your micro service is actually working 😂"
    })


# !-----------------------------------------------------------------------------------------------------------------------------------------------------------------
#* -----------------------------------------------ALL FUNCTIONS FOR DATA ANALYSIS-----------------------------------------------------------------------------------
def get_Hourly_Orders(data, clientName):
    # Step 1: Prepare a nested dictionary {user_name: {hour: count}}
    order_counts = defaultdict(lambda: defaultdict(int))

    for order in data:
        user = order['user_name']
        created_at = datetime.strptime(order['created_at'], "%Y-%m-%d %H:%M:%S")
        hour = created_at.hour  # Extract the hour (0–23)
        order_counts[user][hour] += 1

    # Step 2: Format into a list of dictionaries for DataFrame
    FinalData = []
    for user, hourly_counts in order_counts.items():
        row = {'user_name': user}
        total = 0
        for hour in range(24):
            count = hourly_counts.get(hour, 0)
            row[f'{hour:02d}:00-{(hour+1)%24:02d}:00'] = count
            total += count
        row['Total'] = total
        FinalData.append(row)

    # Step 3: Create DataFrame
    df = pd.DataFrame(FinalData)

    # Step 4: Optional - sort columns by hour
    hour_columns = [f'{hour:02d}:00-{(hour+1)%24:02d}:00' for hour in range(24)]
    df = df[['user_name'] + hour_columns + ['Total']]

    # Step 5: Save to Excel
    excel_file = f"{clientName}_hourly_order_report.xlsx"
    df.to_excel(excel_file, index=False)

    # Load workbook to adjust column widths
    workbook = load_workbook(excel_file)
    sheet = workbook.active

    # Set width of first column ('user_name') to 16
    sheet.column_dimensions[get_column_letter(1)].width = 23

    # Set width of remaining columns (hourly columns + 'Total') to 13
    for col in range(2, sheet.max_column + 1):
        sheet.column_dimensions[get_column_letter(col)].width = 13

    # Save the workbook
    workbook.save(excel_file)

    send_email(excel_file, subject="Hourly orders", clientName=clientName)

def get_Average_Fare(data, clientName):
    output_filename = f"{clientName} Average Fare.xlsx"

    # Step 1: Group absolute amounts by user_name
    user_amounts = defaultdict(list)

    for entry in data:
            user = entry.get('user_name')
            amount_str = entry.get('amount', '0')
            try:
                amount = abs(float(amount_str))
                user_amounts[user].append(amount)
            except ValueError:
                print(f"Skipping invalid amount for user {user}: {amount_str}")
                continue

        # Step 2: Compute averages
    final_data = []
    for user, amounts in user_amounts.items():
            total = sum(amounts)
            avg = sum(amounts) / len(amounts) if amounts else 0
            final_data.append({
                "User Name": user,
                "Average Fare": round(avg, 2)
            })


        # Step 3: Create DataFrame
    df = pd.DataFrame(final_data)

        # Step 4: Save to Excel
    df.to_excel(output_filename, index=False)

        # Step 5: Adjust column widths
    workbook = load_workbook(output_filename)
    sheet = workbook.active

    sheet.column_dimensions[get_column_letter(1)].width = 25  # User_Name
    sheet.column_dimensions[get_column_letter(2)].width = 18  # Average_Amount

    workbook.save(output_filename)
    print(f"Excel file '{output_filename}' generated successfully.")

def get_Number_Of_Orders(data, clientName):
    return

def get_Total_Fare(data, clientName):
    return

def get_Amount_Ranges(data, clientName):
    return

def get_Pickup_Counts_Per_Area(data, clientName):
    return
# !-----------------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    app.run(debug=False)
