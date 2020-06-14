

# Date written: 5/22/3030
# Author: Mark Moore / Sha Kanjoor Anandan
# update: 5/22/2020 code running
# update: 5/23/2020 including code from Sha for service bus
# update: 5/24/2020 added error handling and retry logic to SQL and Service Bus connections
# update: 5/28/2020 updated error handling to remove retry logic write error and fail through
#
# 05/23/2020 Included code from Sha Kanjoor Anandan to push the data to a Service Bus in his Azure subscription

import requests
import json
import pyodbc
import datetime
import logging
import time
from azure.eventhub import EventHubClient, Sender, EventData


# define url to pull json from piaware

url = "http://192.168.0.150/dump1090-fa/data/aircraft.json"

ADDRESS = "amqps://Yournamespace.servicebus.windows.net/Youreventhub"
USER = "YourAccessPolicy"
KEY = "YourAccessPolicyKey"

# Create Event Hubs client
client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
sender = client.add_sender(partition="0")
client.run()

# Main proccessing loop to parse the json to a python dictionary and pull out just the aircraft data.
# Pull out each column and write the columns to SQL, then convert the dictionary containing just the 
# aircraft data back to json and write it into Azure Service Bus.
while True:

    # get request to pull the json data from the piawre computer
    response = requests.get(url)
    
    # convert response to unicode
    data = response.text
    
    # parse the json and convert it into a python dictionary
    parsed = json.loads(data)
    
    # pull the value of now.  It is a time stamp in seconds
    now = parsed["now"]
    messages = parsed["messages"]
    #f.write(str(messages))
    
    # aircraft will contain many rows.  It will contain 1 row per aircraft piaware could see when we sent the get request
    aircraft = parsed["aircraft"]

    # get date.time to use as a column in the database
    currentdt=datetime.datetime.now()

    # loop through rows in aircraft returned from piaware
    # the if statement disregards incomplete records sometimes only partial records are recieved from the transponder
    # pull individual values out of each record and assign the value to variable

    i=0
    while i<len(aircraft):

        record=aircraft[i]
        # Loop through records as a dictionary and populate columns for only complete records
        if "hex" in aircraft[i] and "squawk" in aircraft[i] and "flight" in aircraft[i] and "lat" in aircraft[i] and "lon" in aircraft[i]  and "nucp" in aircraft[i]  and "seen_pos" in aircraft[i]  and "altitude" in aircraft[i]  and "vert_rate" in aircraft[i]  and "track" in aircraft[i]  and "speed" in aircraft[i]  and "category" in aircraft[i]  and "mlat" in aircraft[i]  and "tisb" in aircraft[i]  and "messages" in aircraft[i]  and "seen" in aircraft[i]  and "rssi" in aircraft[i]:
            hex = aircraft[i]['hex']              
            squawk = aircraft[i]['squawk']           # responder id
            flight = aircraft[i]['flight']           # flight number
            lat = aircraft[i]['lat']                 # latitude
            lon = aircraft[i]['lon']                 # longitude
            nucp = aircraft[i]['nucp']               # how strong was the signal from the transponder, higher is better
            seen_pos = aircraft[i]['seen_pos']
            altitude = aircraft[i]['altitude']       # altitude
            vr = aircraft[i]['vert_rate']            # vertical rate negative number are descending positive numbers are ascending
            track = aircraft[i]['track']             # heading as in degrees of the compass
            speed = aircraft[i]['speed']             # speed in knots
            category = aircraft[i]['category']
            mlat = aircraft[i]['mlat']               # boolean (true/ false) if the lon and lat were calcuated.  Requires three or more connected stations
            tisb = aircraft[i]['tisb']
            messages = aircraft[i]['messages']       
            seen = aircraft[i]['seen']
            rssi = aircraft[i]['rssi']

            # convert speed from knots to mph
            speed=speed*1.15078

            #convert python dictionary to json
            eventdatajson=json.dumps(aircraft[i])
            msender.send(EventData(eventdatajson))

        i=i+1






    
