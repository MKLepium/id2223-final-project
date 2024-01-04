import datetime
import pandas as pd
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)


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

def insert_delay_data(cur, date, dev, delay, delay_cutoff_1min, delay_cutoff_2min, delay_cutoff_5min, fer):
    logging.debug(f"Inserting data for date: {date} and dev: {dev}")
    # only insert if the data does not exist

    """
CREATE TABLE bus_data_schema.delay_max_test_v2(
    Date DATE NOT NULL,
    Dev VARCHAR(255) NOT NULL,
    Delay_no_cutoff NUMERIC,
    Delay_cutoff_1min NUMERIC,
    Delay_cutoff_2min NUMERIC,
    Delay_cutoff_5min NUMERIC,
    Fer VARCHAR(255) NOT NULL,
    PRIMARY KEY (Date, Dev, Fer)
);
    """

    sql = """
    INSERT INTO bus_data_schema.delay_max_test_v2 (Date, Dev, Delay_no_cutoff, Delay_cutoff_1min, Delay_cutoff_2min, Delay_cutoff_5min, Fer)
    VALUES (%(date)s, %(dev)s, %(delay)s, %(delay_cutoff_1min)s, %(delay_cutoff_2min)s, %(delay_cutoff_5min)s, %(fer)s)
    ON CONFLICT (date, dev, fer) 
    DO NOTHING
    """

    params = {
        'date': date, 
        'dev': dev, 
        'delay': delay, 
        'delay_cutoff_1min': delay_cutoff_1min, 
        'delay_cutoff_2min': delay_cutoff_2min, 
        'delay_cutoff_5min': delay_cutoff_5min,
        'fer': fer
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
    # For whatever reason I get the dev_list as a list of tuples
    #logging.debug(f"dev_list: {dev_list}")
    modified_list = [item[0] for item in dev_list]

    logging.debug(f"dev_list: {modified_list}")


    for dev in modified_list:
        #logging.debug(f"Processing dev: {dev}")
        
        # 0. get the data and sort it by time
        bus_data_df = get_bus_data(cur, date, dev)
        
        bus_data_df = bus_data_df.sort_values(by='time')

        # 1. identify rows where the stop changes
        bus_data_df['stop_change'] = bus_data_df['stop'] != bus_data_df['stop'].shift(1)

        # 2. stop_change_df contains the timestamps and other data when the bus arrived at a new stop
        stop_change_df = bus_data_df[bus_data_df['stop_change'] & bus_data_df['stop'].notna()].copy()
        logging.debug(f"Stop change df: {stop_change_df}")
        logging.debug(f"Found {len(stop_change_df)} stop changes")
        # 2.1 add the delay column
        stop_change_df['delay'] = None

        # 3. convert the time column to datetime
        stop_change_df['time'] = pd.to_datetime(bus_data_df['time'])
        #print(stop_change_df.head())

        # 4. find the closest scheduled time for each stop change

        logging.debug(f"Processing stop changes")
        logging.debug(f"Stop change df: {stop_change_df.head()}")
        logging.debug(f"Stop change length: {len(stop_change_df)}")

        # Get the unique fer values
        unique_fer_values = stop_change_df['fer'].unique()
        logging.debug(f"Unique fer values: {unique_fer_values}")

        # Create a dictionary to hold the DataFrames
        dfs = {}


        # Loop through each unique value and create a DataFrame for each

        for value in unique_fer_values:
            # Directly create a filtered copy of stop_change_df for the current fer value
            dfs[value] = stop_change_df[stop_change_df['fer'] == value].copy().reset_index(drop=True)

        logging.debug(f"DataFrames: {dfs}")

        for fer, df in dfs.items():
            for index, row in df.iterrows():
                logging.debug(f"Processing stop change row: {row}")
                
                # cast the row['stop'] to int if it is not an int
                if not isinstance(row['stop'], int) and row['stop'].isdigit():
                    row['stop'] = int(row['stop'])
                
                logging.debug(f"schedule_df['stop_id'].dtype, {schedule_df['stop_id'].dtype}")
                logging.debug(f"row['stop'], {type(row['stop'])}")
                matching_schedule_rows = schedule_df[schedule_df['stop_id'] == row['stop']]
                logging.debug(f"Found {len(matching_schedule_rows)} matching schedule rows")


                # If there are no matching rows, continue to the next iteration
                if matching_schedule_rows.empty:
                    logging.debug(f"No matching schedule rows found")
                    continue

                # Get the scheduled arrival times
                schedule_times = matching_schedule_rows['arrival_time'].tolist()
                #logging.debug(f"Schedule times: {schedule_times}")

                # Find the closest schedule time
                closest_time = find_closest_schedule_time(row['time'], schedule_times)
                logging.debug(f"Closest time: {closest_time}")

                # Calculate the delay
                if closest_time:
                    delay = (row['time'] - closest_time).total_seconds() / 60
                    # if delay is negative, the bus arrived early so set delay to 0
                    if delay < 0:
                        delay = 0
                    #stop_change_df.at[index, 'delay'] = delay
                    dfs[fer].at[index, 'delay'] = delay
                    logging.debug(f"Delay: {delay}")
        logging.debug(dfs)
        # 5. calculate the total absolute delay
        # 5.1 remove the rows with no delay
        logging.debug(f"Found {len(stop_change_df)} stop changes before removing rows with no delay")
        for fer, df in dfs.items():
            logging.debug(f"Found {len(df)} stop changes before removing rows with no delay")

            # Filter out rows with NaN in 'delay' and update the dfs dictionary directly
            dfs[fer] = df[df['delay'].notna()]
            logging.debug(f"Found {len(dfs[fer])} stop changes after removing rows with no delay")

            # Now, update df to point to the filtered DataFrame in dfs
            df = dfs[fer]

            # Add new columns
            df['delay_no_cutoff'] = df['delay'].abs()
            df['delay_cutoff_1min'] = df['delay'].abs() > 1
            df['delay_cutoff_2min'] = df['delay'].abs() > 2
            df['delay_cutoff_5min'] = df['delay'].abs() > 5

            # Make sure to update the dictionary with the modified DataFrame
            dfs[fer] = df

        # 5.3 print info
        logging.info(f"Date: {date}")
        logging.info(f"Bus {dev} has a total delay of: {dfs[fer]['delay'].sum()} minutes")
        for fer, df in dfs.items():
            logging.info(f"Fer: {fer}")
            logging.info(f"Bus {dev} has a total delay of: {df['delay'].sum()} minutes")
            logging.info(f"Bus {dev} has a total delay of: {df['delay'].abs().sum()} minutes")
            logging.info(f"Bus {dev} has a total delay of: {df['delay_cutoff_1min'].sum()} minutes with a cutoff of 1 minute")
            logging.info(f"Bus {dev} has a total delay of: {df['delay_cutoff_2min'].sum()} minutes with a cutoff of 2 minutes")
            logging.info(f"Bus {dev} has a total delay of: {df['delay_cutoff_5min'].sum()} minutes with a cutoff of 5 minutes")

        # 5.4 insert the data into the database
        for fer, df in dfs.items():
            insert_delay_data(cur, date, dev, 
                                int(df['delay_no_cutoff'].sum()), 
                                int(df['delay_cutoff_1min'].sum()),
                                int(df['delay_cutoff_2min'].sum()),
                                int(df['delay_cutoff_5min'].sum()),
                                fer)

        





if __name__ == '__main__':
    # read the schedule data
    schedule_df = read_data('gtfs/stop_times.txt')
    schedule_df['stop_id'] = schedule_df['stop_id'].astype(int)
    logging.debug(f"Schedule df: {schedule_df.head()}")
    logging.debug(f"Schedule df info: {schedule_df.info()}")
    logging.debug(f"Schedule df length: {len(schedule_df)}")
    schedule_df['arrival_time'] = schedule_df['arrival_time'].apply(custom_to_datetime).dt.time


    months = ['2023-10', '2023-11', '2023-12']
    for month in months:
        for day in range(1, 32):
            # skip the dates that do not exist
            if month == '2023-10' and day > 31:
                continue
            elif month == '2023-11' and day > 30:
                continue
            elif month == '2023-12' and day > 31:
                continue

            date = f"{month}-{day:02d}"
            logging.info(f"Processing date: {date}")
            transaction_capsule(date, schedule_df)
    logging.info("Done")




