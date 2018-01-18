import datetime, os, calendar
import numpy as np
import pandas as pd
from netCDF4 import date2num,date2index
from netCDF4 import Dataset
import multiprocessing



#stp_yr = np.empty((0,277,349))
#stp_hrs = np.empty((0,39,277,349))
#stp_doy = np.empty((0,1,39,277,349))
#nc = Dataset('/home/data/stp_narr/stp_cin_1979_2017.nc','r',format='NETCDF4_CLASSIC')


# In[3]:


###############################
#Enter the start and end date:#
######################################
#start_input = '1979110100' #YYYYMMDDHH
#end_input  =  '2016110100' #YYYYMMDDHH
######################################
#hours=['00','03','06','09','12','15','18','21']
#hours=['00','03','06','09']
hours=['12','15','18','21']
def makehrly(hrs):
	stp_yr = np.empty((0,277,349))
	stp_hrs = np.empty((0,39,277,349))
	stp_doy = np.empty((0,1,39,277,349))
	nc = Dataset('/home/data/stp_narr/stp_cin_1979_2017.nc','r',format='NETCDF4_CLASSIC')
	months=['01','02','03','04','05','06','07','08','09','10','11','12']
	for month in months:
		if month=='01' or month=='03' or month=='05' or month=='07' or month=='08' or month=='10' or month=='12':
			emon = 31
		elif month=='04' or month=='06' or month=='09' or month=='11':
			emon = 30
		if month=='02':
			emon = 28
		daycount=1
		while daycount <= emon:
			start_input = '1979'+month+'%02d'%daycount+hrs    #YYYYMMDDHH
			end_input  =  '2017'+month+'%02d'%daycount+hrs    #YYYYMMDDHH
			begdate = datetime.datetime.strptime(start_input,"%Y%m%d%H")
			enddate = datetime.datetime.strptime(end_input,"%Y%m%d%H")
			dates = []
			while begdate <= enddate:
				if calendar.isleap(begdate.year) == True:   
					dates.append(begdate)
					begdate+=datetime.timedelta(days=1)
				else:
					dates.append(begdate)
				begdate+=datetime.timedelta(days=365)
			#do things here
			#print dates
			#print len(dates)
			for i,dt in enumerate(dates):
				idex = date2index(dt,nc['time'])
				stp = nc.variables["stp"][idex][:][:]
				cin  = nc.variables["sbcin"][idex][:][:]
				#cin mask
				term5 = np.fabs(cin)
				term5[np.fabs(cin)>50]=0.
				term5[np.fabs(cin)<=50]=1.
				stp = stp * term5
				stp[stp<0]=0.
				stp[stp>=500]=0.
				stp_yr = np.append(stp_yr,[stp],axis=0)
				#print scp_climo.shape
			#print stp_yr.shape
			#print stp_hrs.shape
			del idex
			del stp
			del cin
			del term5
			stp_hrs=np.append(stp_hrs,[stp_yr],axis=0)
			del stp_yr
			stp_yr = np.empty((0,277,349))
			#print stp_hrs.shape
			#print stp_doy.shape
			stp_doy=np.append(stp_doy,[stp_hrs],axis=0)
			del stp_hrs
			stp_hrs = np.empty((0,39,277,349))
			daycount+=1
	#print (stp_doy.shape)
	#stp_doy = np.sum(stp_doy,axis=2)
	np.save('/home/data/stp_narr/stp_'+hrs+'z.npy',stp_doy)

numprocs=multiprocessing.cpu_count()
pool=multiprocessing.Pool(processes=numprocs)
r2=pool.map(makehrly,hours)
pool.close()
