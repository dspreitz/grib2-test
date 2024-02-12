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

ds0 = xr.open_dataset("saved_on_disk.nc")
# EDNC: 49.021205,11.4835479
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

