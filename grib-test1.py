import xarray as xr
import cfgrib
import os, glob
import requests

import urllib.request
from urllib.error import URLError, HTTPError
import bz2
# import json
import math
import os
from datetime import datetime, timedelta, timezone
import logging as log
# from extendedformatter import ExtendedFormatter
import concurrent.futures
from concurrent.futures.thread import ThreadPoolExecutor




global dryRun
global compressed
global skipExisting
global maxWorkers
skipExisting = True
dryRun = None
compressed = False
retainDwdTree = True
dwdPattern = "{model!L}/grib/{modelrun:>02d}/{param!L}"
failedFiles = []

def getCurrentRun(now_utc):

        '''Gets the number of the current run by current time. 
        The latest run is fully available ~2h after initialisation.
        e.g. The 09 run will be finished at approx 11:00 UTC.

        Parameters
        ----------
        now_utc : datetime
            The current datetime

        Returns
        -------
        string
            The run hour as a string (00, 03, 06 ...)
        '''

        h = now_utc.hour
        run_hour = "00"

        if h >= 2:
            run_hour = "00"
        if h >= 5:
            run_hour = "03"
        if h >= 8:
            run_hour = "06"
        if h >= 11:
            run_hour = "09"
        if h >= 14:
            run_hour = "12"
        if h >= 17:
            run_hour = "15"
        if h >= 20:
            run_hour = "18"
        if h >= 24 or h < 2:
            run_hour = "21"

        # self._currentRun = run_hour

        return run_hour


def downloadAndExtractBz2FileFromUrl(url, destFilePath=None, destFileName=None):

    if dryRun:
        log.debug("Pretending to download file: '{0}' (dry-run)".format(url))
        return
    else:
        log.debug("Downloading file: '{0}'".format(url))

    if destFileName == "" or destFileName == None:
        # strip the filename from the url and remove the bz2 extension
        destFileName = url.split('/')[-1].split('.bz2')[0]

    if destFilePath == "" or destFilePath == None:
        destFilePath = os.getcwd()

    if compressed:
        fullFilePath = os.path.join(destFilePath, destFileName + '.bz2')
    else:
        fullFilePath = os.path.join(destFilePath, destFileName)
    if skipExisting and os.path.exists(fullFilePath):
        log.debug("Skipping existing file: '{0}'".format(fullFilePath))
        return fullFilePath

    try:
        resource = urllib.request.urlopen(url)
        compressedData = resource.read()
        if compressed:
            binaryData = compressedData
        else:
            binaryData = bz2.decompress(compressedData)

        log.debug("Saving file as: '{0}'".format(fullFilePath))
        with open(fullFilePath, 'wb') as outfile:
            outfile.write(binaryData)
        return fullFilePath
    except HTTPError as e:
        log.error(f"Downloading failed. Reason={e}, URL={url}")
        failedFiles.append((url, e.status, HTTPError))
    except Exception as e:
        log.exception(f"Downloading failed. Reason={e}, URL={url}")

# pressure_levels = [200, 250, 300, 400, 500, 600, 700, 850, 950, 975, 1000]

para = ["t","qv","p"]
CurrentRun = str(getCurrentRun(datetime.now(timezone.utc)))
time = str(datetime.now().strftime('%Y%m%d')) + CurrentRun
# print(time)
level_start = 40 # highest level, starting with level 1
level_end = 66 # lowest level, ending with level 65
# Loop over timesteps
for t in range(0, 3):
       file_list_t = []
       file_list_qv = []
       file_list_p = []
       print(t)

       # Loop over model levels
       for i in range(level_start, level_end):
                  # https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/t/icon-d2_germany_regular-lat-lon_model-level_2024021212_000_40_t.grib2.bz2
             url = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/"+CurrentRun+"/t/icon-d2_germany_regular-lat-lon_model-level_"+time+"_"+str(t).zfill(3)+"_"+str(i)+"_t.grib2.bz2"
             # print(url)
             result = downloadAndExtractBz2FileFromUrl(url)
             file_list_t.append(result)

       print(*file_list_t, sep = "\n")
       d0 = xr.open_mfdataset(file_list_t, engine='cfgrib', combine="nested", concat_dim="generalVerticalLayer")
       print(d0)

       # Get qv (specific humidity) over all model levels
       for i in range(level_start, level_end):
             #      https://opendata.dwd.de/weather/nwp/icon-d2/grib/15/qv/icon-d2_germany_regular-lat-lon_model-level_2024020915_006_27_qv.grib2.bz2
             url = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/"+CurrentRun+"/qv/icon-d2_germany_regular-lat-lon_model-level_"+time+"_"+str(t).zfill(3)+"_"+str(i)+"_qv.grib2.bz2"
             # print(url)
             result = downloadAndExtractBz2FileFromUrl(url)
             file_list_qv.append(result)

       print(*file_list_qv, sep = "\n")
       d1 = xr.open_mfdataset(file_list_qv, engine='cfgrib', combine="nested", concat_dim="generalVerticalLayer")
       print(d1)

       # Get p (pressure) over all model levels
       for i in range(level_start, level_end):
             #      https://opendata.dwd.de/weather/nwp/icon-d2/grib/15/p/icon-d2_germany_regular-lat-lon_model-level_2024021015_006_6_p.grib2.bz2
             url = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/"+CurrentRun+"/p/icon-d2_germany_regular-lat-lon_model-level_"+time+"_"+str(t).zfill(3)+"_"+str(i)+"_p.grib2.bz2"
             # print(url)
             result = downloadAndExtractBz2FileFromUrl(url)
             file_list_p.append(result)

       print(*file_list_p, sep = "\n")
       d2 = xr.open_mfdataset(file_list_p, engine='cfgrib', combine="nested", concat_dim="generalVerticalLayer")
       print(d2)


       # Merge data of one timestep
       ds = xr.concat([d0, d1, d2], dim=["latitude","longitude"], data_vars="minimal")
       print(ds)
       print("Zeitstempel: ")
       print(ds.coords['valid_time'].values)

       # Merge the timesteps
       if t == 0:
             ds0 = ds
       else:
             # Merge Timesteps
             ds0 = xr.concat([ds0, ds], dim="time", data_vars="minimal")
       print(ds0)

# print(ds0.coords['valid_time'].values)

ds0.to_netcdf("saved_on_disk.nc")

# ds0 = xr.open_dataset("saved_on_disk.nc")
# Geographical Position of EDNC: 49.021205,11.4835479
print("%%%%%%%%%%%%%%%%%%%%%%%%")
print(ds0)
print("%%%%%%%%%%%%%%%%%%%%%%%%")
print(ds0.coords.values)
for t in ds0.coords['valid_time'].values:
       print(t)
       ds0.sel(time=t, method='nearest').values
       for p in ds0.data_vars:
             print(p)
             # print(ds0.sel(time=t).interp(latitude=49.021205, longitude=11.4835479)[p].values)
             # print(ds0.sel(latitude=49.021205, longitude=11.4835479, method='nearest', valid_time=t)[p].values)


quit()


# EDNC: 49.021205,11.4835479
print("%%%%%%%%%%%%%%%%%%%%%%%%")
for t in ds0.coords['time'].values:
       print(t)
       for p in ds0.data_vars:
              print(ds0.sel(latitude=49.021205, longitude=11.4835479, time=t, method='nearest')[p])
              print(ds0.sel(latitude=49.021205, longitude=11.4835479, time=t, method='nearest')[p].values)

quit()
# Load single dataset
# ds0 = xr.load_dataset("icon-d2_germany_regular-lat-lon_model-level_2024020615_000_10_t.grib2", engine="cfgrib")

# Put all downloaded files in lists
file_list_t = glob.glob('*_t.grib2')
file_list_relhum = glob.glob('*_relhum.grib2')
print(file_list_t)
print(file_list_relhum)

# Alt-Layer go from model level 1 - 65
# Alt-Layer go from pressure level 200, 250, 300, 400, 500, 600, 700, 850, 950, 975, 1000, 
# Time Steps go from 000 to 048
# Load multiple altitude datasets of timestep1
d0 = xr.open_mfdataset(file_list_t, engine='cfgrib', combine="nested", concat_dim="generalVerticalLayer")
print(d0)

# Load multiple altitude datasets of timestep2
d1 = xr.open_mfdataset(file_list_relhum, engine='cfgrib', combine="nested", concat_dim="generalVerticalLayer")
print(d1)

# Merge multiple timesteps
ds = xr.concat([d0,d1], dim="time")

print(ds)


# Cleaning up, deleting *.idx files
for f in glob.glob("*.idx"):
    os.remove(f)

quit()

