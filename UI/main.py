# gradio application
import gradio as gr
# datetime
import datetime
import requests
import xml.etree.ElementTree as ET

MODELNAME = "model_name"
MODELVERSION = "1.0"

# Dunno if you actually need this method, but here you go
def get_model(name, version):
    pass

def predict(input):
    pass


# example input string: 01.10.2023
# example output datetime: 2023-10-01
def get_date_from_string(date_str: str):
    date_format = "%d.%m.%Y"
    date = datetime.datetime.strptime(date_str, date_format)
    return date




def create_row(input):
    # input is a dict
    # should have: date, weekday, temp_min, temp_max, temp_avg, fer
    # extracted from the txt
    fers = ['7-B', '1-I', 'A1', '4-G', '1-D', '55-A', '1-G', '3-B', '79-A', '22-A', '105-A', '1-H', '88-A', '12-H', '24-A', '17-A', '56-A', '52-A', '1-E', '104-A', '11-E', '89-A', '2-B', '4-E', '12-B', '13-C', '5-B', '18-C', '71-A', '14-E', '2-C', '1-B', '16-B', '3-C', '14-C', '36-A', '15-C', '4-A', '106-A', '15-E', 'A4', '55-B', '3-D', '21-C', '2-E', '57-B', '51-E', 'A6-B', '55-C', '12-F', '3-R', '6-D', '15-D', '16-A', '8-A', '2-G', 'A6', '15-H', '21-A', '15-F', '78-A', '14-D', '12-G', '3-E', '18-A', 'A5', '1A-R', '18-E', '24-C', '11-B', '59-A', '51-B', '28-B', '35-A', '1-A', '13-A', '11-A', '4-B', '55-D', '58-A', '51-A', '17-B', '1-K', '31-B', '6-B', '18-F', '1-F', '82-A', '12-D', '2-F', '1-J', '14-F', '13-E', '11-C', '6-F', '1-L', '19-B', '51-D', '57-D', '101-A', '2-D', '3-A', '7-A', '57-A', '51-C', '14-B', '72-A', 'A2', '19-A', '6-E', '13-B', '12-C', '57-C', '4-F', '5-E', '15-A', '4-C', 'A3', '6-C', '1B-R', '23-A', '18-G', '73-A', '2-A', '12-E', '31-A', '11-D', '21-B', '1-C', '28-A', '11-F', '5-D', '24-B', '81-A', '1-R', '3-F', '15-G', '103-A', '13-D', '6-A', '18-D', '5-C', '5-A', '16-C', '15-B', '14-A', '64-A', '18-B', '4-D', '12-A']
    

    # output should be:
    # monday (bool), tuesday (bool), wednesday (bool), thursday (bool), friday (bool), saturday (bool), sunday (bool), temp_min, temp_max, temp_avg, and for each fer a bool
    fer_dict = {}
    for fer in fers:
        fer_dict[fer] = False

    row = {
        "date": input['date'],
        "monday": input['weekday'] == 0,
        "tuesday": input['weekday'] == 1,
        "wednesday": input['weekday'] == 2,
        "thursday": input['weekday'] == 3,
        "friday": input['weekday'] == 4,
        "saturday": input['weekday'] == 5,
        "sunday": input['weekday'] == 6,
        "temp_min": 0,
        "temp_max": 0,
        "temp_avg": 0,
    }
    row.update(fer_dict)
    # pretty print
    #for key, value in row.items():
    #    print(f"\"{key}\": {value},")
    
    # adjust row based on the input
    date = input["date"]
    weekday = input["weekday"]
    temp_min = input["temp_min"]
    temp_max = input["temp_max"]
    temp_avg = input["temp_avg"]
    fer = input["fer"]

    row["temp_min"] = temp_min
    row["temp_max"] = temp_max
    row["temp_avg"] = temp_avg

    row["monday"] = weekday == 0
    row["tuesday"] = weekday == 1
    row["wednesday"] = weekday == 2
    row["thursday"] = weekday == 3
    row["friday"] = weekday == 4
    row["saturday"] = weekday == 5
    row["sunday"] = weekday == 6

    for fer in fers:
        row[fer] = fer == input["fer"]


    #for key, value in row.items():
    #    print(f"\"{key}\": {value},")

    return row

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
    print(len(weather_data))
    today = datetime.datetime.now()
    tomorrow = today + datetime.timedelta(days=1)
    tomorrow_date_string = tomorrow.strftime("%Y-%m-%d")

    weather_data_tomorrow = []
    for forecast in weather_data:
        # Check if the forecast's time starts with the date of tomorrow
        if forecast['time'].startswith(tomorrow_date_string):
            weather_data_tomorrow.append(forecast)
    print(len(weather_data_tomorrow))
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
    print(temp_min)
    print(temp_max)
    print(temp_avg)
    return temp_min, temp_max, temp_avg


# for when we need to get the temp from the database
def get_temp_for_date(date):
    pass

def process_input_data(date='2023-10-01', temp_min=-10, temp_max=10, temp_avg=0, fer='7-B', use_tomorrows_weather_forecast=True):
    # Convert string date to datetime object
    date = datetime.datetime.strptime(date, '%Y-%m-%d')
    weekday = date.weekday()
    # Process data as needed
    input = {
        "date": date,
        "weekday": weekday,
        "temp_min": temp_min,
        "temp_max": temp_max,
        "temp_avg": temp_avg,
        "fer": fer
    }
    row = create_row(input)
    output = predict(row)

    return output


"""
Sample input:
input = {
    "date": date,
    "weekday": date.weekday(),
    "temp_min": temp_min,
    "temp_max": temp_max,
    "temp_avg": temp_avg,
    "fer": "7-B"
}
"""
if __name__ == "__main__":
    model = get_model(MODELNAME, MODELVERSION)

    # 01.10.2023
    date = get_date_from_string("01.10.2023")
    temp_min, temp_max, temp_avg = get_temp_for_tomorrow()

    """gr.inputs.Date(label="Date"),
    gr.inputs.Number(label="Minimum Temperature"),
    gr.inputs.Number(label="Maximum Temperature"),
    gr.inputs.Number(label="Average Temperature"),
    gr.inputs.Textbox(label="FER")"""
    interface = gr.Interface(
        fn=process_input_data,
        inputs=[
            "text",
            "text",
            "text",
            "text",
            "text",
            "checkbox"
        ],
        examples=[
            ["2023-10-01", "-10", "10", "0", "7-B", False],
            ["2024-01-02", "-10", "0", "-8", "7-B", True],
        ],
        outputs="json"
    )
    interface.launch()