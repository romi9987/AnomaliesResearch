from configparser import ConfigParser
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymongo   
from pymongo import MongoClient


parser = ConfigParser()
_ = parser.read('notebook.cfg')

#point the client on localhost
client = MongoClient(parser.get('my_api', 'conn'))

#select database
db = client[parser.get('my_api', 'database')]

#select the collections within the database
visit = db[parser.get('my_api', 'visit_collection')]
event = db[parser.get('my_api', 'event_collection')]
event_chaoticmovement = db[parser.get('my_api', 'event_chaoticmovement_collection')]
event_highest_scroll = db[parser.get('my_api', 'event_highest_scroll_collection')]
event_leftclick = db[parser.get('my_api', 'event_leftclick_collection')]
event_mousemove = db[parser.get('my_api', 'event_mousemove_collection')]
event_rageclick = db[parser.get('my_api', 'event_rageclick_collection')]
event_ragekeypress = db[parser.get('my_api', 'event_ragekeypress_collection')]
event_refreshing = db[parser.get('my_api', 'event_refreshing_collection')]
event_zoom = db[parser.get('my_api', 'event_zoom_collection')]


#convert entire collection to Pandas dataframe
visit = pd.DataFrame(list(visit.find()))
event = pd.DataFrame(list(event.find()))
event_chaoticmovement = pd.DataFrame(list(event_chaoticmovement.find()))
event_highest_scroll = pd.DataFrame(list(event_highest_scroll.find()))
event_leftclick = pd.DataFrame(list(event_leftclick.find()))
event_mousemove = pd.DataFrame(list(event_mousemove.find()))
event_rageclick = pd.DataFrame(list(event_rageclick.find()))
event_ragekeypress = pd.DataFrame(list(event_ragekeypress.find()))
event_refreshing = pd.DataFrame(list(event_refreshing.find()))
event_zoom = pd.DataFrame(list(event_zoom.find()))


#Zmieniam opcje max_columns i max_rows

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 20000
#pd.reset_option("display.max_columns")
#pd.reset_option("display.max_rows")

# ### Visit Data Preparation

#Tworze project_id

project_id = str(visit['projectId'][0])


#Zamieniam datetype w string

visit['createdAt'] = visit['createdAt'].astype(str)


#W journeys zamieniam Nan na '0'

visit["journeys"] = visit["journeys"].replace(np.nan,'0')


#Tworze liste z journeys

list_journeys = visit['journeys'].to_list()


#Zamieniam bson.objectid.ObjectId z journeys w string 

a = 0
b = 0
journey_list = []

while a < len(list_journeys):
    if type(list_journeys[a])==str:
        journey_str = '0'
        journey_list.append(journey_str)           

    elif len(list_journeys[a]) > 1:
        dict_list = []
        for numb in range(0,len(list_journeys[a])):
            journey_str = list_journeys[a][numb]
            journey_str = str(journey_str)
            dict_list.append(journey_str)

        journey_list.append(dict_list)
    else:
        journey_str = list_journeys[a][b]
        journey_str = str(journey_str)
        journey_list.append(journey_str)
    a+=1
#Wyciagam string z 1-elementowej listy

journeys_list = []

for journey_str in journey_list:
    if type(journey_str) == list and len(journey_str) == 1:
        journey_str = journey_str[0]
        journeys_list.append(journey_str)
    else:
        journeys_list.append(journey_str)


visit["journeys"] = journeys_list[:]


#Tworze listy z kolumn visit
id_list = visit['_id'].to_list()
dates_list = visit['createdAt'].to_list()
journeys_list = visit['journeys'].to_list()


#using zip() to convert lists to dictionary
date_journey_dict = dict(zip(dates_list, journeys_list))


#Wyciagam journeys z poszczegolnych list dwu lub wiecej-elementowych.
createdAt_list = []

for i, (k, v) in enumerate(date_journey_dict.items()):
    if type(date_journey_dict[k]) == list:
        for numb in range(0,len(date_journey_dict[k])):
            k = k + '-' + str(numb)
            createdAt_list.append(k)
    else:
        createdAt_list.append(k)


journeys_list = []

for i, (k, v) in enumerate(date_journey_dict.items()):
    if type(date_journey_dict[k]) == list:
        for numb in range(0,len(date_journey_dict[k])):
            v = date_journey_dict[k][numb]
            journeys_list.append(v)
    else:
        journeys_list.append(v)


#Tworze DataFrame z list _id createdAt i journeys

date_journey = pd.DataFrame(list(zip(id_list, createdAt_list,journeys_list)))
date_journey.columns =['_id' ,'createdAt', 'journeys']


#Pozbywam sie journeys i createdAt z visit

visit = visit.drop(['createdAt' ,'journeys'],axis=1)


#Outer merge

visit = pd.merge(visit,date_journey,how='outer',on='_id')


#Sort values on createdAt

visit = visit.sort_values('createdAt')


#W kolumnie projectId zamieniam Nan na project_id

visit["projectId"] = visit["projectId"].replace(np.nan, project_id)


#W kolumnie _id zamieniam ObjectId w string

visit["_id"] = visit["_id"].astype(str)


visit = visit.sort_values('createdAt')


visit = visit.drop(["_id", "userId", "slug", "agent", "ip", "status", "dcV", "origin", "playable", "entryPage", "timeframe", "gtmCapture", "gauidCapture", "exitPage"],axis=1)


# ### Event Data Preparation

#Zamieniam ObjectId w string

event["createdAt"] = event["createdAt"].astype(str)

event = event.drop(["_id", "sessionId", "socketId", "visitId", "projectId"],axis=1)

event = event.sort_values('createdAt')


# ### Highest Scroll Data Preparation

event_highest_scroll["createdAt"] = event_highest_scroll["createdAt"].astype(str)

event_highest_scroll["pattern"] = "highest_scroll"


# ### Mousemove Data Preparation

event_mousemove["createdAt"] = event_mousemove["createdAt"].astype(str)

event_mousemove["pattern"] = "m_move"


# ### Leftclick Data Preparation

event_leftclick["createdAt"] = event_leftclick["createdAt"].astype(str)

event_leftclick["pattern"] = "l_click"


# ### Rageclick Data Preparation

event_rageclick["createdAt"] = event_rageclick["createdAt"].astype(str)

event_rageclick["pattern"] = "rageclick"


# ### Ragekeypress Data Preparation

event_ragekeypress["createdAt"] = event_ragekeypress["createdAt"].astype(str)

event_ragekeypress["pattern"] = "ragekeypress"


# ### Refreshing Data Preparation

event_refreshing["createdAt"] = event_refreshing["createdAt"].astype(str)

event_refreshing["pattern"] = "refreshing"

# ### Zoom Data Preparation

event_zoom["createdAt"] = event_zoom["createdAt"].astype(str)

event_zoom["pattern"] = "zoom"


# ### Event_pattern merge with event

event_highest_scroll = event_highest_scroll[["pattern", "createdAt"]]

event_mousemove = event_mousemove[["pattern", "createdAt"]]

event_leftclick = event_leftclick[["pattern", "createdAt"]]

event_rageclick = event_rageclick[["pattern", "createdAt"]]

event_zoom = event_zoom[["pattern", "createdAt"]]

event_ragekeypress = event_ragekeypress[["pattern", "createdAt"]]

event_refreshing = event_refreshing[["pattern", "createdAt"]]


event_pattern = pd.concat([event_highest_scroll, event_mousemove, event_leftclick, event_rageclick, event_zoom, event_ragekeypress, event_refreshing])
event_pattern = event_pattern.reset_index(drop=True)


event_pattern = event_pattern.sort_values('createdAt')


event = pd.merge(event, event_pattern, how='outer',on='createdAt')


event = event.sort_values('createdAt')


# ### Data Merge on createdAt

#Outer merge

visit = pd.merge(event, visit, how='outer', on='createdAt')


visit = visit.sort_values('createdAt')

#W journeys zamieniam Nan na -1

visit["journeys"] = visit["journeys"].replace(np.nan,-1)


# ### Data Preparation

event = visit[visit['journeys']!=-1]


event = event.sort_values('createdAt')


date_list = event["createdAt"].to_list()


dates_list = []

for date in date_list:
    day = date[:10]
    dates_list.append(day)


event["date"] = dates_list[:]
event["date"] = pd.to_datetime(event["date"], format='%Y-%m-%d')


#One Hot - pattern

dummy_variable = pd.get_dummies(event["pattern"])
event = pd.concat([event, dummy_variable], axis=1)
event = event.drop("pattern", axis = 1)


#One Hot - action

dummy_variable_1 = pd.get_dummies(event["action"])
event = pd.concat([event, dummy_variable_1], axis=1)
event = event.drop("action", axis = 1)

#One Hot - devicePlatform

dummy_variable_2 = pd.get_dummies(event["devicePlatform"])
event = pd.concat([event, dummy_variable_2], axis=1)
event = event.drop("devicePlatform", axis = 1)


#One Hot - deviceOs

event["deviceOs"] = event["deviceOs"].replace("",-1)
event["deviceOs"] = event["deviceOs"].replace("Ubuntu","Linux")
event["deviceOs"] = event["deviceOs"].replace("Chromium OS","Linux")
event["deviceOs"] = event["deviceOs"].replace("Fedora","Linux")


dummy_variable_3 = pd.get_dummies(event["deviceOs"])
event = pd.concat([event, dummy_variable_3], axis=1)
event = event.drop("deviceOs", axis = 1)


#One Hot - deviceBrowser

event["deviceBrowser"] = event["deviceBrowser"].replace("",-1)
event["deviceBrowser"] = event["deviceBrowser"].replace("GSA","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("MIUI Browser","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("Chrome Headless","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("Instagram","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("WebKit","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("Chrome WebView","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("Android Browser","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("Opera Touch","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("Yandex","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("UCBrowser","OtherB")
event["deviceBrowser"] = event["deviceBrowser"].replace("Falkon","OtherB")


dummy_variable_4 = pd.get_dummies(event["deviceBrowser"])
event = pd.concat([event, dummy_variable_4], axis=1)
event = event.drop("deviceBrowser", axis = 1)

#One Hot - deviceVendor

event["deviceVendor"] = event["deviceVendor"].replace("",-1)
event["deviceVendor"] = event["deviceVendor"].replace("OPPO","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("LG","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Realme","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Nokia","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Lenovo","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("HTC","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Sony","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("ASUS","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Google","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("OnePlus","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Vivo","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("ZTE","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("MEIZU","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Meizu","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Acer","OtherV")
event["deviceVendor"] = event["deviceVendor"].replace("Fairphone","OtherV")


dummy_variable_5 = pd.get_dummies(event["deviceVendor"])
event = pd.concat([event, dummy_variable_5], axis=1)
event = event.drop("deviceVendor", axis = 1)


pages = event['pages'].to_list()

number_list = []

for page in pages:
    if type(page) == list:
        number = len(page)
        number_list.append(number)
    elif type(page) == str:
        number = 1
        number_list.append(number)
    else:
        number_list.append(page)
    
event['pages_nb'] = number_list[:]


referrer_list = event["referrer"].tolist()

number_list = []

for referrer in referrer_list:
    if referrer != 0:
        number = 1
        number_list.append(number)
    else:
        number_list.append(referrer)
    
event['referrer_nb'] = number_list[:]



#Event analyse all

event_analyse = event[["projectId", "date", "journeys", "return", "pageViews", "tabs", "duration", "engageTime", "m_move", "mousemove", "scroll", "desktop", "mobile", "tablet", "console", "smarttv", "Andr>

journey_nb = event_analyse[["date", "journeys"]].value_counts()
journeys_nb = pd.DataFrame(journey_nb)
journeys_nb = journeys_nb.reset_index()
journeys_nb.columns = ["date", "journeys", 'counts']
journeys_nb = journeys_nb.sort_values('date').reset_index(drop=True)

journeys_nb["date_str"] = journeys_nb["date"].astype(str)

percent = journeys_nb['counts'] / journeys_nb.groupby('date')['counts'].transform('sum')
journeys_nb["percent"] = percent

event_analyse["counts"] = journeys_nb["counts"]
event_analyse["percent"] = journeys_nb["percent"]

event_analyse = event_analyse.sort_values('date').reset_index(drop=True)

event_analyse_grouped = event_analyse.groupby(['projectId', 'date', 'journeys'], as_index=False).mean()


# ### Event analyse

 length = 0

while length < len(event_analyse_grouped):
    mean_return_project = event_analyse_grouped.iloc[length]['projectId']
    mean_return_date = event_analyse_grouped.iloc[length]['date']
    mean_return_journey = event_analyse_grouped.iloc[length]['journeys']
    mean_return_count = event_analyse.iloc[length]['counts']
    mean_return_percent = event_analyse.iloc[length]['percent']
    for column in event_analyse_grouped.columns[3:]:
        mean_return_value = event_analyse_grouped.iloc[length][column]
        db.journey_mean.insert_one({"projectId":mean_return_project, "date":mean_return_date, "journey":mean_return_journey, "counts":mean_return_count, "percent":mean_return_percent, column:mean_return_v>
    length+=1
                      
