import pandas as pd
import gspread
import requests
import base64
import json
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
from pandas.io.json import json_normalize
from datetime import date, datetime, timedelta

# Load credentials and API settings from file.
with open('./credentials/api_credentials.json') as json_file:
    data = json.load(json_file)
spreadsheet_key = data['spreadsheet_key']
fyber_credentials = data['fyber']

# FYBER credentials and stuff.
endpoint = "https://api.fyber.com/publishers/v2/reporting/publisher-kpis"
encodedBytes = base64.b64encode(fyber_credentials.encode("utf-8"))
encodedString = str(encodedBytes, "utf-8")
basicCredentials = "Basic " + encodedString

# Define date range to get data for (recent days are not complete on date, so just get the past 7 days) 
since = datetime.today() - timedelta(days=7)
until = date.today()

# paramaters for request to Fyber.
parameters = {'since':since,'until':until}

# Check output.
response = requests.get(endpoint, headers={'Authorization': basicCredentials}, params = parameters)
print('Response from Fyber: ' + str(response))

# GOOGLE SHEETS
# Credentials and stuff
scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('./credentials/service_account_credentials.json', scope)
gc = gspread.authorize(credentials)

print('Google credentials ok.')

# Extract JSON, select relevant data.
df = response.json()
df = json_normalize(df['data'])
df = df[['date','ad_format','impressions','revenue_eur']] 

# Arrange said data
df = df.groupby(['date','ad_format'],as_index=False).sum()
df['platform'] = "mobile"
df = df[['date','platform','ad_format','impressions','revenue_eur']]

# Pick worksheet to insert into
wks_name = 'Jupyter Manipulated Data'

#define offset based on dawn of time that we care about
dawnOfTime = date(2018,1,1)
firstDateInDataframe = datetime.strptime(df['date'][1], '%Y-%m-%d').date()    
rowOffset = firstDateInDataframe - dawnOfTime
rowOffset = rowOffset.days

# Fyber limits API calls to 30 days of data, so offset insertion based on first date of dataframe
startCell = 'A'+str(rowOffset*2+1) # times two because we have two rows per date

print('Data inserted at cell: ' + startCell)

# Upload the whole thing to our google sheet
d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, clean=False,start_cell=startCell,row_names=False,col_names=False)