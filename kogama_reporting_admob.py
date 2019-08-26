#!/usr/bin/python
#
# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Based on Google sample code.
# Collect AdMob revenue reporting data from AdSense and inserts data into a google sheet for charting purposes.

import pandas as pd
import sys
import json
import gspread
from apiclient import sample_tools
from oauth2client import client
from datetime import date, datetime,timedelta
from pandas.io.json import json_normalize
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g

# Authenticate and construct service.
service, flags = sample_tools.init(argv='',
  name='adsense', version='v1.4',doc='doc', filename='file', parents=[],
  scope='https://www.googleapis.com/auth/adsense.readonly')

# The AdMob Mediation Bill is passed. The system goes online on August 18th, 2019.
# Human decisions are removed from strategic defense. AdMob begins to learn at a geometric rate.
start_date = "2019-08-18"
end_date = str(date.today())

try:
    # Let the user pick account if more than one.
    account_id = '[YourAccountID]' # get_account_id(service)

    # Retrieve report.
    result = service.accounts().reports().generate(accountId=account_id, startDate=start_date, endDate=end_date,
                                                       metric=['VIEWED_IMPRESSIONS', 'EARNINGS'],
                                                       dimension=['DATE','AD_UNIT_NAME'],
                                                      sort=['+DATE']).execute()
except client.AccessTokenRefreshError:
    print ('The credentials have been revoked or expired, please re-run the application to re-authorize')

df = pd.DataFrame(result['rows'])
df.columns=['date','ad_format','impressions','revenue_DKK']


# Google Sheets credentials and stuff
scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('./credentials/service_account_credentials.json', scope)
gc = gspread.authorize(credentials)

print('Google credentials ok.')

# Load settings from file.
with open('./credentials/api_credentials.json') as json_file:
    data = json.load(json_file)
spreadsheet_key = data['spreadsheet_key']

# Arrange data
df['platform'] = "mobile"
df['provider'] = "admob"
df = df[['date','provider','platform','ad_format','impressions','revenue_DKK']]

# Pick worksheet to insert into
wks_name = 'Jupyter AdMob Data'

# Upload the whole thing to our google sheet.
d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, clean=True,row_names=False,col_names=False)

print ('Data uploaded to sheet.')
