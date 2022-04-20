import sqlite3
import json
import os
import matplotlib.pyplot as plt
import requests
from datetime import datetime
import numpy as np



def crime_api_call():
    result = requests.get("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/3/query?where=OFFENSE%20%3D%20'ASSAULT%20W%2FDANGEROUS%20WEAPON'%20OR%20OFFENSE%20%3D%20'HOMICIDE'&outFields=REPORT_DAT,OFFENSE&outSR=4326&f=json")
    response = result.json()
    return response

def get_crime_date_and_type(response):
    list_of_dic = []
    for item in response['features']:
            if 1614564000000 < item['attributes']['REPORT_DAT'] < 1625014800000:
                    unix = (item['attributes']['REPORT_DAT'])/1000
                    d = datetime.fromtimestamp(unix)
                    date = d.strftime('%Y-%m-%d')
                    offense = item['attributes']['OFFENSE']
                    dictionary = {}
                    dictionary['date'] = date
                    dictionary[offense] = 1
                    list_of_dic.append(dictionary)
    return list_of_dic

def crime_org(list_of_dic):
    crimes_per_date = []
    for item in list_of_dic:
        new_dic = {}
        new_dic['date'] = item['date']
        new_dic['ASSAULT W/DANGEROUS WEAPON'] = 0
        new_dic['HOMICIDE'] = 0
        crimes_per_date.append(new_dic)
    for dict in crimes_per_date:
        for item in list_of_dic:
            if dict['date'] == item['date']:
                try:
                    if item['ASSAULT W/DANGEROUS WEAPON'] == 1:
                        dict['ASSAULT W/DANGEROUS WEAPON'] += 1
                except:
                    if item['HOMICIDE'] == 1:
                        dict['HOMICIDE'] += 1
    return crimes_per_date

def weather_api_call():
    base_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/DC/2021-03-01/2021-06-30?unitGroup=us&elements=datetime%2Ctemp&include=days&key=JRBRCLM73L4BL92D2EAWXAY5M&contentType=json'
    result = requests.get(base_url)
    response = result.json()
    weather_temp_json = json.dumps(response, indent = 4)
    return response

def rain_api_call():
    base_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/DC/2021-03-01/2021-06-30?unitGroup=us&elements=datetime%2Cprecip&include=days&key=JRBRCLM73L4BL92D2EAWXAY5M&contentType=json'
    result = requests.get(base_url)
    response = result.json()
    rain_json = json.dumps(response, indent = 4)
    return response

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def setUpCrimeTable(data, cur, conn):
    cur.execute("""CREATE TABLE IF NOT EXISTS Crime(
        date TEXT UNIQUE PRIMARY KEY,
        assaults TEXT,
        homicides INTEGER) """)
    conn.commit()
    for i in data:
        date = i['date']
        assaults = i['ASSAULT W/DANGEROUS WEAPON']
        homicides = i['HOMICIDE']
        cur.execute("""INSERT OR IGNORE INTO Crime(
            date, assaults, homicides)
            VALUES (?,?,?)""", (date, assaults, homicides))
        conn.commit()

def setUpTemperatureTable(data, cur, conn):
    cur.execute("""CREATE TABLE IF NOT EXISTS Temperature(
        date TEXT UNIQUE PRIMARY KEY,
        temperature INTEGER) """)
    conn.commit()
    for i in data['days']:
        date = i['datetime']
        temperature = i['temp']
        cur.execute("""INSERT OR IGNORE INTO Temperature(
            date, temperature)
            VALUES (?,?)""", (date, temperature))
        conn.commit()

def setUpPrecipTable(data, cur, conn):
    cur.execute("""CREATE TABLE IF NOT EXISTS Precipitation(
        date TEXT UNIQUE PRIMARY KEY,
        precipitation_inches INTEGER) """)
    conn.commit()
    for i in data['days']:
        date = i['datetime']
        precip = i['precip']
        cur.execute("""INSERT OR IGNORE INTO Precipitation(
            date, precipitation_inches)
            VALUES (?,?)""", (date, precip))
        conn.commit()

def crimeVtemp_plot(cur, conn):
    cur.execute("""SELECT Crime.assaults, Crime.homicides, Temperature.temperature
    FROM crime JOIN temperature
    ON Crime.date = Temperature.date
    """)
    data = cur.fetchall()
    assaults = []
    temp = []
    homicides = []
    for i in data:
        assaults.append(i[0])
        homicides.append(i[1])
        temp.append(i[2])
    x = np.array(temp)
    y = np.array(assaults)
    plt.scatter(temp, assaults, c='blue', label= 'assaults')
    x = np.array(temp)
    y = np.array(homicides)
    plt.scatter(temp, homicides, c='hotpink', label='homicides')
    plt.legend()
    plt.ylabel("Number of Assaults and Homicides per Day")
    plt.xlabel("Temperature in Degrees Farenheit")
    plt.title("Number of Violent Crimes versus Temperature")
    plt.tight_layout()
    plt.show()

def crimeVprecip_plot(cur, conn):
    cur.execute("""SELECT Crime.assaults, Crime.homicides, Precipitation.precipitation_inches
    FROM crime JOIN precipitation
    ON Crime.date = Precipitation.date
    """)
    data = cur.fetchall()
    assaults = []
    precip = []
    homicides = []
    for i in data:
        assaults.append(i[0])
        homicides.append(i[1])
        precip.append(i[2])
    x = np.array(precip)
    y = np.array(assaults)
    plt.scatter(precip, assaults, c='blue', label= 'assaults')
    x = np.array(precip)
    y = np.array(homicides)
    plt.scatter(precip, homicides, c='hotpink', label='homicides')
    plt.legend()
    plt.ylabel("Number of Assaults and Homicides per Day")
    plt.xlabel("Precipitation in Inches")
    plt.title("Number of Violent Crimes versus Precipitation")
    plt.tight_layout()
    plt.show()

def crimesPerDayPlot(cur, conn): #group by month
    cur.execute("""SELECT date, SUM(assaults), SUM(homicides)
    FROM Crime
    GROUP BY date
    """)
    data = cur.fetchall()
    dates = []
    num_crimes = []
    for i in data:
        dates.append(i[0][8:])
        num_crimes.append(i[1])
    plt.bar(dates, num_crimes, color='purple', width=.3)
    plt.xlabel('Date')
    plt.ylabel('Number of Violent Crimes Reported')
    plt.title('Number of Violent Crimes Reported Daily in March 2021')
    plt.show()

def FindAverages(cur, conn): #update this
    cur.execute('SELECT assaults FROM Crime')
    assault_data = cur.fetchall()
    sum_assaults = 0
    for item in assault_data:
        sum_assaults += int(item[0])
    avg_assaults = sum_assaults/31
    cur.execute('SELECT homicides FROM Crime')
    homicide_data = cur.fetchall()
    sum_homicides = 0
    for item in homicide_data:
        sum_homicides += int(item[0])
    avg_homicides = sum_homicides/31
    cur.execute('SELECT precipitation_inches FROM Precipitation')
    precip_data = cur.fetchall()
    sum_precip = 0
    for item in precip_data:
        sum_precip += int(item[0])
    avg_precip = sum_precip/31
    cur.execute('SELECT temperature FROM Temperature')
    temp_data = cur.fetchall()
    sum_temp = 0
    for item in temp_data:
        sum_temp += int(item[0])
    avg_temp = sum_temp/31
    return [avg_assaults, avg_homicides, avg_precip, avg_temp]


def writeFile(filename, cur, conn): #update this
    l = FindAverages(cur, conn)
    f = open(filename, 'w')
    f.write(f"Average Number of Reported Assaults in Washington D.C. During March 2021: {l[0]}")
    f.write(f"Average Number of Reported Homicides in Washington D.C. During March 2021: {l[1]}")
    f.write(f"Average Amount of Precipitation (in) in Washington D.C. During March 2021: {l[2]}")
    f.write(f"Average Temperature in Washington D.C. During March 2021: {l[3]}")
    f.close()



def main():
        response = crime_api_call()
        list_of_dic = get_crime_date_and_type(response)
        data = crime_org(list_of_dic)
        # temp_json = weather_api_call()
        # precip_json = rain_api_call()
        cur, conn = setUpDatabase('FinalProject.db')
        setUpCrimeTable(data, cur, conn)
        # setUpTemperatureTable(temp_json, cur, conn)
        # setUpPrecipTable(precip_json, cur, conn)
        crimeVtemp_plot(cur, conn)
        crimeVprecip_plot(cur,conn)
        # crimesPerDayPlot(cur, conn)
        # FindAverages(cur, conn)
        # writeFile('FinalProject.txt', cur, conn)


main()








