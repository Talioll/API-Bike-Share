from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd



app = Flask(__name__) 

#ROUTE 

# ROUTE: STATIONS
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods = ['POST'])
def route_add_stations():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result


# POST Enndpoint
@app.route('/count_id_duration', methods = ['POST'])
def route_id_duration():
    input_data = request.get_json(force= True) 
    specified_date = input_data['period'] 

 
    conn = make_connection()
    query = f"SELECT * FROM trips WHERE start_time LIKE '{specified_date}%'"
    selected_data = pd.read_sql_query(query, conn)


    result = selected_data.groupby('start_station_name').agg({
    'id' : 'count', 
    'duration_minutes' : 'mean'})
    return result.to_json() 



# ROUTE: TRIPS

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trips_id>')
def route_trips_id(trips_id):
    conn = make_connection()
    trip = get_trips_id(trips_id, conn)
    return trip.to_json()

@app.route('/trips/add', methods = ['POST'])
def route_add_trips():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

# ROUTE: SUBSCRIBER TYPE

@app.route('/subscriber_type/frequency')
def route_subs_freq():
    conn = make_connection()
    subs_freq = subscriber_freq(conn)
    return subs_freq.to_json()

# ROUTE: DURATION TYPE

@app.route('/duration')
def route_duration():
    conn = make_connection()
    duration = duration_avg(conn)
    return duration.to_json()


@app.route('/duration_subscriber')
def route_duration_subscriber():
    conn = make_connection()
    duration_subs = avg_duration(conn)
    return duration_subs.to_json()



# ROUTE: START & END STATION FREQUENCY

@app.route('/start_station/freq')
def route_start_station_freq():
    conn = make_connection()
    start_station = start_stat_freq(conn)
    return start_station.to_json()

@app.route('/end_station/freq')
def route_end_station_freq():
    conn = make_connection()
    end_station = end_stat_freq(conn)
    return end_station.to_json()

# ROUTE: TIME FREQUENCY

@app.route('/start_time/<hour>')
def route_hour(hour):
    conn = make_connection()
    hour_freq = get_hour(hour, conn)
    return hour_freq.to_json()

@app.route('/year_freq/<year>')
def route_year(year):
    conn = make_connection()
    year_freq = get_year(year, conn)
    return year_freq.to_json()


# FUNCTION

# GLOBAL FUNCTION
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

conn = make_connection()


# FUNCTION: STATIONS PAGE
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'



# FUNCTION: TRIPS PAGE

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trips_id(trips_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trips_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'


# FUNCTION: SUBSCRIBER TYPE PAGE


def subscriber_freq(conn):
    query = f"""select subscriber_type, count(subscriber_type) as Total from trips
            group by subscriber_type 
            order by Total desc;"""
    result = pd.read_sql_query(query, conn)
    return result


# FUNCTION: DURATION PAGE


def duration_avg(conn):
    query = f"""select round(AVG(duration_minutes)) as Average, 
            min(duration_minutes) as Minimum, 
            max(duration_minutes) as Maximum from trips"""
    result = pd.read_sql_query(query, conn)
    return result


def avg_duration(conn):
    query = f"""select subscriber_type, round(avg(duration_minutes)) as Average
            From Trips
            Group by subscriber_type
            order by Average desc
            """
    result = pd.read_sql_query(query, conn)
    return result



# FUNCTION: START & END STATION FREQUENCY

def start_stat_freq(conn):
    query = f"""select start_station_name, count(start_station_name) as Total
            from trips
            group by start_station_name
            order by Total desc"""
    result = pd.read_sql_query(query, conn)
    return result

def end_stat_freq(conn):
    query = f"""select end_station_name, count(end_station_name) as Total
            from trips
            group by end_station_name
            order by Total desc"""
    result = pd.read_sql_query(query, conn)
    return result



# TIME FREQUENCY
def get_hour(hour, conn):
    query = f"""select count(start_time) as total
            from trips
            where CAST(SUBSTR(start_time, 12, 2) AS INTEGER) == {hour}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_year(year, conn):
    query = f"""select count(start_time) as total
            from trips
            where CAST(SUBSTR(start_time, 1, 4) AS INTEGER) == {year}"""
    result = pd.read_sql_query(query, conn)
    return result 





if __name__ == '__main__':
    app.run(debug=True, port=5000)


