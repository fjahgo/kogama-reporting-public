# Import the needed libraries and stuff
import tempfile
import pandas as pd
import io
import gzip
import gspread
from googleads import ad_manager, errors
from datetime import date, datetime,timedelta
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
from pandas.io.json import json_normalize

# Initialize a client object, by default uses the credentials in ~/googleads.yaml.
client = ad_manager.AdManagerClient.LoadFromStorage()

# Initialize a DataDownloader.
report_downloader = client.GetDataDownloader(version='v201902')

# Initialize a ReportService.
report_service = client.GetService('ReportService', version='v201902')

# Set the start and end dates of the report to run (past 8 days).
start_date = date(2018,1,1)
end_date = date.today()

report_job = {
    'reportQuery':
    {
        'dimensions': ['DATE', 'DEVICE_CATEGORY_NAME', 'PLACEMENT_NAME'],
        'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'],
        'adUnitView': 'HIERARCHICHAL',
        'dateRangeType': 'CUSTOM_DATE',
        'startDate': start_date,
        'endDate': end_date
    }
}

try:
    # Run the report and wait for it to finish.
    report_job_id = report_downloader.WaitForReport(report_job)
except errors.AdManagerReportError as e:
    print('Failed to generate report. Error was: %s' % e)
    
# Change to your preferred export format.
# Note that file downloads in zipped format
export_format = 'CSV_DUMP'

# Name a temp file. Requires import of "tempfile"
report_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)

# Download report data.
report_downloader.DownloadReportToFile(report_job_id, export_format, report_file)

report_file.seek(0)

with open(report_file.name, 'rb') as fd:
    gzip_fd = gzip.GzipFile(fileobj=fd)
    df = pd.read_csv(gzip_fd)
    
# Pass data on to google sheet.
scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('./[YourCredentialsFile.json]', scope)
gc = gspread.authorize(credentials)
spreadsheet_key = '[YourSpreadsheetKey]'

# Arrange data.
df = df.groupby(['Dimension.DATE','Dimension.PLACEMENT_NAME','Dimension.DEVICE_CATEGORY_NAME'],as_index=False).sum()
df['platform'] = "desktop"
df = df[['Dimension.DATE','platform','Dimension.PLACEMENT_NAME','Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE','Dimension.DEVICE_CATEGORY_NAME']]

# Pick worksheet to insert into.
wks_name = '[PickANameForYourWorkSheet]'

# Upload the whole thing to our google sheet.
d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, clean=True,row_names=False,col_names=False)
