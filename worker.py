import requests
import psycopg2
import psycopg2.extras
from datetime import datetime
import logging

from keys import api_token
from keys import db_password

def fetch_data():
    url = 'http://api.wunderground.com/api/' + api_token + '/conditions/q/NC/Raleigh.json'
    r = requests.get(url).json()
    data = r['current_observation']

    location = data['observation_location']['full']     # city, state, and observation location
    weather = data['weather']                           # cloud, clear, etc
    wind_str = data['wind_string']                      # pretty print version of wind speed and dir
    temp = data['temp_f']                               # temp in fahrenheit
    humidity = data['relative_humidity']                # humidity with %
    precip = data['precip_today_string']                # displays total precip in inches and mm
    icon_url = data['icon_url']                         # url for weather icon (clear, cloud, rainy, etc.)
    observation_time = data['observation_time']         # pretty print of observation time

    #open db
    try:
        conn = psycopg2.connect(dbname='weather', user='postgres', host='localhost', password=db_password)
        print('Opened DB Successfully')
    except:
        print(datetime.now(), "Unable to connect to the database")
        logging.exception("Unable to open the database.")
        return
    else:
        cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

    # write data to database.
    cur.execute("""INSERT INTO station_reading(location, weather, wind_str, temp, humidity, precip, icon_url, observation_time) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)""", (location, weather, wind_str, temp, humidity, precip, icon_url, observation_time))
    conn.commit()
    cur.close()
    conn.close()

    print("Data Written", datetime.now())

fetch_data()