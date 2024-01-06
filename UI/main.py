# gradio application
import gradio as gr
# datetime
import datetime
import requests
import xml.etree.ElementTree as ET
import hopsworks
import joblib
import pandas as pd
from db import DBManager
import os

MODELNAME = "bus_model"
MODELVERSION = 1
MODEL_FILE = "model.pkl"

DAY_PREFIX = "day_"
FER_PREFIX = "fer_"


# download model
project = hopsworks.login(project="ID2223_MKLepium")
mr = project.get_model_registry()
model = mr.get_model(MODELNAME, version=MODELVERSION)
"""
path = os.path.join(MODELNAME, MODEL_FILE)
if not os.path.exists(path):
    model_dir = model.download()
    # You never actually loaded saved the model, the download puts it in a temp dir
    model = joblib.load(model_dir + "/" + MODEL_FILE)
    model.save(path)

model = joblib.load(path)
"""
print("Model downloaded")

schema = model.model_schema


# transform schema to pandas dataframe
columns = [column['name'] for column in schema['input_schema']['columnar_schema']]
columns_df = pd.DataFrame(columns=columns)
for column in schema['input_schema']['columnar_schema']:
    columns_df[column['name']] = columns_df[column['name']].astype(column['type'])

print("Columns")
print(columns_df)


# download individual ferries
with DBManager() as db:
    query = """
        SELECT DISTINCT fer FROM delay_max_test_v2 ORDER BY fer
    """
    cur = db.conn.cursor()
    cur.execute(query)
    ferries = cur.fetchall()
    ferries = [fer[0] for fer in ferries]


# get dates of training data
# from start time 2023-10-01 00:00:00 to end time 2023-12-31 23:59:59
start_date = datetime.datetime(2023, 10, 1, 0, 0, 0)
end_date = datetime.datetime(2023, 12, 31, 23, 59, 59)
dates = [("None", None)]
dates += [
    (date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d")) for date in
    [start_date + datetime.timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]
]

def predict(fer)-> str:
    """Takes a ferry and a date and returns a delay prediction"""
    df = columns_df.copy()

    # set all values to false
    df.loc[0] = False
    row = df.loc[0]

    # get temp
    temp_min, temp_max, temp_avg = get_temp_for_tomorrow()
    row['tmin'] = temp_min
    row['tmax'] = temp_max
    row['tavg'] = temp_avg

    # get day name for date
    today = datetime.datetime.now()
    tomorrow = today + datetime.timedelta(days=1)
    day_name = tomorrow.strftime("%A")

    # set day to true
    day_key = DAY_PREFIX + day_name
    row[day_key] = True

    # set fer to true
    fer_key = FER_PREFIX + fer
    row[fer_key] = True

    # set row in dataframe
    df.loc[0] = row

    # get prediction
    prediction = model.predict(df)

    return prediction[0]

def get_weather_forecast():
    url = "https://xmlweather.vedur.is/?op_w=xml&type=forec&lang=en&view=xml&ids=1;27"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        return "Error: Unable to fetch data"

def parse_weather_data(xml_data):
    # Parse the XML data
    root = ET.fromstring(xml_data)

    # List to store all forecasts
    forecasts = []

    # Iterate over each forecast element in the XML
    for forecast in root.iter('forecast'):
        # Create a dictionary for each forecast
        forecast_data = {
            'time': forecast.find('ftime').text,
            'wind_speed': forecast.find('F').text,
            'wind_direction': forecast.find('D').text,
            'temperature': forecast.find('T').text,
            'weather': forecast.find('W').text
        }
        # Add the dictionary to the list of forecasts
        forecasts.append(forecast_data)

    return forecasts

def get_temp_for_tomorrow():
    weather_data = get_weather_forecast()
    weather_data = parse_weather_data(weather_data)
    today = datetime.datetime.now()
    tomorrow = today + datetime.timedelta(days=1)
    tomorrow_date_string = tomorrow.strftime("%Y-%m-%d")

    weather_data_tomorrow = []
    for forecast in weather_data:
        # Check if the forecast's time starts with the date of tomorrow
        if forecast['time'].startswith(tomorrow_date_string):
            weather_data_tomorrow.append(forecast)
    # calculate the average, min and max temperature
    temp_min = 100
    temp_max = -100
    temp_sum = 0
    for forecast in weather_data_tomorrow:
        temp = float(forecast['temperature'])
        temp_sum += temp
        if temp < temp_min:
            temp_min = temp
        if temp > temp_max:
            temp_max = temp
    temp_avg = temp_sum / len(weather_data_tomorrow)
    return temp_min, temp_max, temp_avg


# for when we need to get the temp from the database
def get_temp_for_date(date):
    pass

demo = gr.Interface(
    fn=predict,
    inputs=[
        gr.Dropdown(ferries, label="Select Ferry"),
        # gr.Dropdown(dates, label="Select Historical Day"),
    ],
    outputs=gr.Textbox(label="Delay Information")
)

# Run the Gradio interface
demo.launch(server_port=8090, server_name="0.0.0.0")