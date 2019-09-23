################################################################################
# Script Created by William Leatham IV in September of 2019
# Description:
# This program was created to grab the fischer porter data and put it into a CSV
# file for the OPL to determine the sites that need attention. In addition the 
# COOP data is grabbed from ACIS to compare the Fischer porter data to the COOP.
#
################################################################################
# Import a few modules required
import math 
import pandas as pd
from pandas import Timestamp
import os, sys
import re
import numpy as np
from datetime import datetime, timedelta
import csv
import urllib2
import json
from Tkinter import Tk
import tkFileDialog

configcsv = 'station_config.csv'
outputdir = os.getcwd() 
current_time = datetime.utcnow()
mnth = int(current_time.strftime('%m'))-1

# Making sure the year is correct.
if mnth == 0:
	mnth = 12
	year = int(current_time.strftime('%Y'))-1
else:
	year = int(current_time.strftime('%Y'))

# Opening up the original CSV file and returning it to a list, skipping the
# headers 
def stationidentifier(filename):
	with open(filename) as csvfile:
		readcsv = csv.reader(csvfile,delimiter=',')
# Skipping the header
		header = next(readcsv)
# Opening up an empty array
		stationlist = []
		for row in readcsv:
			snum = row[0]
# Splicing the original snum to only have the state identifier steid and number
# identifier for the station.
			steid = snum[0:2]+'00'
			numid = snum[3:7]
			stid = row[1]
			sloc = row[2]
			stvl = row[3]
			mcnt = row[4]
			edvl = row[5]
			totl = row[6]
			full = row[7]
			qual = row[8]
			stationlist.append([snum,steid+numid,stid,sloc,stvl,mcnt,edvl,totl,full,
		qual])
		return stationlist

# Opening up the fischer porter data
def fischerdata(directory):
	os.chdir(directory)
	dsetdic = {}
	colname = ['STID & DATE','TIME','PRECIP','VAL1','VAL2','VAL3','VAL4','VAL5']

	for files in os.listdir(directory):
		if files.endswith('.TXT'):
# Here we read in each text file as a csv, but skip the first line of the file
# as it is empty. 
			df = pd.read_csv(files, skiprows=1)
			df.columns = colname
# Splitting up the STID & Date column and making two new columns. Then the 
# pound symbol is removed from the STID column. Combined the date and time. Then
# I rearranged the order of the df. Here I removed all other data except the 
# STID, date and precipitation. 
			df[['STID','DATE']] = df['STID & DATE'].str.split(' ',expand=True)
			df['STID'] = df['STID'].map(lambda x: x.lstrip('#'))
			df['DATE'] = df['DATE'].map(str) + df['TIME']
			df = df[['STID','DATE','PRECIP']]
 
# Here I change the index of the dataframe so it is a timestamp. If there is an error in the timestamp
# then that row is set to null. This is done by errors = 'coerce'. 
			df['DATE']= pd.to_datetime(df['DATE'],format='%y/%m/%d%H:%M:%S',utc=True,errors='coerce')
			df.set_index('DATE',inplace=True)
# Calculating the monthly values, first we gather the first and the last value
# from the month. 
			frstmon = df.groupby(df.index.month).apply(lambda x: x.iloc[[0]])
			frstmon.reset_index(level=1, drop=True,inplace=True)

			lastmon = df.groupby(df.index.month).apply(lambda x: x.iloc[[-1]])
			lastmon.reset_index(level=1, drop=True,inplace=True)

# This adds the first monthly precipitation value to the last monthly precipitation
# value dataframe. Then I take the difference to get the monthly total precipitation
# which is under the DIFF column. Then I add the last value of the original dataframe
# to the table. 
			lastmon.insert(2,"FRSTPRECIP",frstmon.PRECIP)
			lastmon['DIFF'] = lastmon.PRECIP - lastmon.FRSTPRECIP
			lastmon.insert(4,"LASTVALUE",df.PRECIP[-1])

# Putting the station ID, the previous months monthly precipitation, the last 
# value of the file and the percentage of the gauge that is full. 
			dsetdic.update({lastmon.STID[mnth]:
				[lastmon.DIFF[mnth],lastmon.LASTVALUE[mnth],(lastmon.LASTVALUE[mnth]/19.0)*100]})
	return dsetdic

# Gathering the coop data from rcc-acis. 
def getcoopdata(coopsite):

	url = 'http://data.rcc-acis.org/StnData'
	date = str(year)+'-'+str(mnth)
# Sending data request to the rcc-acis server
	def make_request(url,params):
		req = urllib2.Request(url,
		json.dumps(params),
		{"Content-Type":"application/json"})

# Checking to see if we get any error codes in our data request. If we do then 
# the error code is displayed.
		try:
			response = urllib2.urlopen(req)
			return json.loads(response.read())
		except urllib2.HTTPError as error:
			if error.code == 400: print error.msg
	def stnsrv(params):
		return make_request(url,params)
# Making the call for the monthly precipitation data. 
	params = {
		"sid":coopsite,
		"sdate":date,
		"edate":date,
		"elems":[{"name":"pcpn",
			"interval":"mly",
			"duration":"mly",
			"reduce":{
				"reduce":"sum",
				"add":"mcnt"
			}
		}]
	}
	coopdata = stnsrv(params)
    
	jsondf = json.dumps(coopdata,indent=4)
    
	jsonparsed = json.loads(jsondf)
	mnthlyprecip = jsonparsed["data"][0][1][0]
	missingcount = jsonparsed["data"][0][1][1]
	return mnthlyprecip, missingcount
    
# Main module that creates the csv file. 
def main():
	
	header = ['Station Number','Identifier','STID','Location','SRG Value','COOP Msg','End Value','Total','%Full','Quality']
	statsdf = pd.DataFrame(stationidentifier(configcsv), columns = header)

# Creating a Tk object
	s = Tk()
	s.withdraw()
	
# Ask the user to select where the fischer porter .txt files are located
	fischerdir = tkFileDialog.askdirectory(initialdir="F:\CPM\DATA\Fischer Porter Data\HPD",title='Please select where the fischer porter text files are located')

	data = fischerdata(fischerdir)

	os.chdir(outputdir)
# Looping through the dataframe row by row. 
	for row in statsdf.itertuples():

# Adding the COOP data to the dataframe.
		srgdata,missingcount = getcoopdata(row.STID)
		statsdf.at[row.Index,'SRG Value'] = srgdata
		statsdf.at[row.Index,'COOP Msg'] = missingcount

# Looping through the keys from the data dictionary, then where the key matches
# the Identifier column the data is filled in. 
		for key in data:
			if key == row.Identifier:
				statsdf.at[row.Index,'Total'] = round(data[key][0],2)
				statsdf.at[row.Index,'End Value'] = round(data[key][1],2)
				statsdf.at[row.Index,'%Full'] = round(data[key][2],2)

# Removing the Identifier column of the dataframe so the format is the same as 
# the original file.
	del statsdf['Identifier']
# Putting NaN in any columns where the data is blank. 
	rcols = ['SRG Value','COOP Msg','End Value','Total','%Full']
	statsdf[rcols] = statsdf[rcols].replace({'':np.nan})
# Sorting the dataframe by the %Full then writing it to a file
	finaldf = statsdf.sort_values('%Full',ascending=False)
	finaldf.to_csv('Calculator-'+current_time.strftime('%Y-%B')+'.csv',index=False)

main()
