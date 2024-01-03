# read the data from the csv file and create pandas dataframe
# give me a function to read all the txt files in a directory and give me each file as a pandas dataframe
import datetime
import pandas as pd
import psycopg2
import logging

logging.basicConfig(level=logging.DEBUG)


def read_data(file):
    df = pd.read_csv(file)
    return df

def custom_to_datetime(time_str):
    if time_str.startswith("24:"):
        # Convert "24:xx:xx" to "00:xx:xx" and add a day
        adjusted_time = "00" + time_str[2:]
        return pd.to_datetime(adjusted_time, format='%H:%M:%S') + pd.Timedelta(days=1)
    elif time_str.startswith("25:"):
        # Convert "25:xx:xx" to "01:xx:xx" and add a day
        adjusted_time = "01" + time_str[2:]
        return pd.to_datetime(adjusted_time, format='%H:%M:%S') + pd.Timedelta(days=1)
    elif time_str.startswith("26:"):
        # Convert "26:xx:xx" to "02:xx:xx" and add a day
        adjusted_time = "02" + time_str[2:]
        return pd.to_datetime(adjusted_time, format='%H:%M:%S') + pd.Timedelta(days=1)
    elif time_str.startswith("27:"):
        # Convert "27:xx:xx" to "03:xx:xx" and add a day
        adjusted_time = "03" + time_str[2:]
        return pd.to_datetime(adjusted_time, format='%H:%M:%S') + pd.Timedelta(days=1)
    elif time_str.startswith("28:"):
        # Convert "28:xx:xx" to "04:xx:xx" and add a day
        adjusted_time = "04" + time_str[2:]
        return pd.to_datetime(adjusted_time, format='%H:%M:%S') + pd.Timedelta(days=1)
    else:
        return pd.to_datetime(time_str, format='%H:%M:%S')

def find_closest_schedule_time(actual_time, schedule_times):
    actual_datetime = datetime.datetime.combine(actual_time.date(), actual_time.time())
    min_diff = float('inf')
    closest_time = None
    for scheduled_time in schedule_times:
        scheduled_datetime = datetime.datetime.combine(actual_time.date(), scheduled_time)
        diff = abs((scheduled_datetime - actual_datetime).total_seconds())
        if diff < min_diff:
            min_diff = diff
            closest_time = scheduled_datetime
    return closest_time


# Meh- solution.
# Only reachable from localhost so security
db_params = {
    'dbname': 'bus_data',
    'user': 'postgres',
    'password': 'db_password',
    'host': '127.0.0.1',
    'port': '5432'
}

def get_bus_data(cur, date, dev):
    logging.debug(f"Getting data for date: {date} and dev: {dev}")
    sql = "SELECT * FROM bus_data WHERE DATE(time) = %(date)s AND dev = %(dev)s"
    # call the postgresql database and get the data
    # return the data as a pandas dataframe
    param = {'date': date, 'dev': dev}
    cur.execute(sql, param)
    rows = cur.fetchall()
    logging.debug(f"Found {len(rows)} rows")

    #print(rows)
    #print(len(rows))
    #print(type(rows))

    # create a pandas dataframe
    """
Column |            Type             | Collation | Nullable | Default
--------+-----------------------------+-----------+----------+---------
 time   | timestamp without time zone |           | not null |
 lat    | numeric(12,9)               |           |          |
 lon    | numeric(12,9)               |           |          |
 head   | character varying(255)      |           |          |
 fix    | character varying(255)      |           |          |
 route  | character varying(255)      |           |          |
 stop   | character varying(255)      |           |          |
 next   | character varying(255)      |           |          |
 code   | character varying(255)      |           |          |
 dev    | character varying(255)      |           | not null |
 fer    | character varying(255)      |           |          |

    """
    df = pd.DataFrame(rows, columns=['time', 'lat', 'lon', 'head', 'fix', 'route', 'stop', 'next', 'code', 'dev', 'fer'])
    #print(df.head())
    #print(df.info())
    #print(df.describe())

    return df

def insert_delay_data(cur, date, dev, delay, delay_cutoff_1min, delay_cutoff_2min, delay_cutoff_5min):
    logging.debug(f"Inserting data for date: {date} and dev: {dev}")
    sql = "INSERT INTO bus_data_schema.delay_max_test VALUES (%(date)s, %(dev)s, %(delay)s, %(delay_cutoff_1min)s, %(delay_cutoff_2min)s, %(delay_cutoff_5min)s)"
    params = {
        'date': date, 
        'dev': dev, 
        'delay': delay, 
        'delay_cutoff_1min': delay_cutoff_1min, 
        'delay_cutoff_2min': delay_cutoff_2min, 
        'delay_cutoff_5min': delay_cutoff_5min
    }
    logging.debug(f"Executing sql: {sql} with params: {params}")
    cur.execute(sql, params)

    #conn.commit()
    
def transaction_capsule(date, schedule_df):
    logging.debug(f"Starting transaction for date: {date}")
    try:
        # SELECT * FROM bus_data WHERE DATE(time) = '2023-11-01';
        execute_sql = "SELECT DISTINCT dev FROM bus_data WHERE DATE(time) = %(date)s"
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        param = {'date': date}
        cur.execute(execute_sql, param)
        logging.debug(f"Executing sql: {execute_sql} with params: {param}")
        dev_list = cur.fetchall()
        logging.debug(f"Found {len(dev_list)} devices")
        process_date(date, schedule_df, dev_list, cur)
        conn.commit()
    except Exception as e:  # Catching all exceptions
        logging.error(f"Error at date: {date}")
        logging.error(e)
        conn.rollback()
    finally:
        conn.close()
        logging.debug(f"Committed & Closed connection for date: {date}")

        
def process_date(date, schedule_df, dev_list, cur):
    logging.debug(f"dev_list: {dev_list}")

    for dev in dev_list:
        logging.debug(f"Processing dev: {dev}")
        
        # 0. get the data and sort it by time
        bus_data_df = get_bus_data(cur, date, dev)
        
        bus_data_df = bus_data_df.sort_values(by='time')

        # 1. identify rows where the stop changes
        bus_data_df['stop_change'] = bus_data_df['stop'] != bus_data_df['stop'].shift(1)

        # 2. stop_change_df contains the timestamps and other data when the bus arrived at a new stop
        stop_change_df = bus_data_df[bus_data_df['stop_change'] & bus_data_df['stop'].notna()].copy()
        logging.debug(f"Found {len(stop_change_df)} stop changes")
        # 2.1 add the delay column
        stop_change_df['delay'] = None

        # 3. convert the time column to datetime
        stop_change_df['time'] = pd.to_datetime(bus_data_df['time'])
        #print(stop_change_df.head())

        # 4. find the closest scheduled time for each stop change

        for index, row in stop_change_df.iterrows():
            logging.debug(f"Processing stop change row: {row}")
            matching_schedule_rows = schedule_df[schedule_df['stop_id'] == row['stop']]
            logging.debug(f"Found {len(matching_schedule_rows)} matching schedule rows")


            # If there are no matching rows, continue to the next iteration
            if matching_schedule_rows.empty:
                logging.debug(f"No matching schedule rows found")
                continue

            # Get the scheduled arrival times
            schedule_times = matching_schedule_rows['arrival_time'].tolist()
            logging.debug(f"Schedule times: {schedule_times}")

            # Find the closest schedule time
            closest_time = find_closest_schedule_time(row['time'], schedule_times)
            logging.debug(f"Closest time: {closest_time}")

            # Calculate the delay
            if closest_time:
                delay = (row['time'] - closest_time).total_seconds() / 60
                stop_change_df.at[index, 'delay'] = delay
                logging.debug(f"Delay: {delay}")
        #print(stop_change_df)
        
        # 5. calculate the total absolute delay
        # 5.1 remove the rows with no delay
        logging.debug(f"Found {len(stop_change_df)} stop changes before removing rows with no delay")
        stop_change_df = stop_change_df[stop_change_df['delay'].notna()]

        # 5.2 create a new row for delay_no_cutoff, delay_cutoff_1min, delay_cutoff_2min, delay_cutoff_5min
        # 5.2.1 delay_no_cutoff
        stop_change_df['delay_no_cutoff'] = stop_change_df['delay'].abs()

        # 5.2.2 delay_cutoff_1min
        # Assign 1 if absolute delay is greater than 1, else 0
        stop_change_df['delay_cutoff_1min'] = stop_change_df['delay'].abs() > 1

        # 5.2.3 delay_cutoff_2min
        # Assign 1 if absolute delay is greater than 2, else 0
        stop_change_df['delay_cutoff_2min'] = stop_change_df['delay'].abs() > 2

        # 5.2.4 delay_cutoff_5min
        # Assign 1 if absolute delay is greater than 5, else 0
        stop_change_df['delay_cutoff_5min'] = stop_change_df['delay'].abs() > 5



        # 5.3 print info
        logging.info(f"Date: {date}")
        logging.info(f"Bus {dev} has a total delay of: {stop_change_df['delay_no_cutoff'].sum()} minutes")
        logging.info(f"Bus {dev} has a total delay of: {stop_change_df['delay_cutoff_1min'].sum()} minutes with a cutoff of 1 minute")
        logging.info(f"Bus {dev} has a total delay of: {stop_change_df['delay_cutoff_2min'].sum()} minutes with a cutoff of 2 minutes")
        logging.info(f"Bus {dev} has a total delay of: {stop_change_df['delay_cutoff_5min'].sum()} minutes with a cutoff of 5 minutes")

        # 5.4 insert the data into the database
        insert_delay_data(cur, date, dev, 
                          stop_change_df['delay_no_cutoff'].sum(), 
                          stop_change_df['delay_cutoff_1min'].sum(), 
                          stop_change_df['delay_cutoff_2min'].sum(), 
                          stop_change_df['delay_cutoff_5min'].sum())





if __name__ == '__main__':
    # read the schedule data
    schedule_df = read_data('gtfs/stop_times.txt')
    schedule_df['arrival_time'] = schedule_df['arrival_time'].apply(custom_to_datetime).dt.time
    transaction_capsule('2023-11-01', schedule_df)
    print("Done")



