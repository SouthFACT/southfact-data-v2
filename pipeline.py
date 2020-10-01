from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import io, argparse
import contextlib
import json
import re
import threading
import time
import uuid
import time
import ee
import subprocess, datetime, os, pdb
from osgeo import gdal
from os.path import isfile, join

def parseCmdLine():
    # Will parse the arguments provided on the command line.
    parser = argparse.ArgumentParser(description='Download GEE products and create regional GeoTIFF')
    parser.add_argument('downloadDir', help='download directory e.g., C:/Users/smith/Downloads/ ')
    parser.add_argument('outputGeoTIFFDir', help='GeoTIFF directory e.g., G:/latestChange/')
    parser.add_argument('ids_file', help='the fullpath where export to Drive task ids are stored e.g., G:/productTranscript.txt')
    parser.add_argument('drive_key_file', help='the fullpath of the token.pickle file e.g., G:/keys/token.pickle')
    parser.add_argument('credentials_file', help='the fullpath of the credentials file e.g., C:/Users/smith/credentials.json')    
    ns = parser.parse_args()
    return [ns.downloadDir, ns.outputGeoTIFFDir, ns.ids_file, ns.drive_key_file, ns.credentials_file]

# Utility function to mosaic a list of rasters
# @param
#     [inRasterList] - a list of rasters
#     [outRasterPath] - full path to the output raster
# @return None
# Example call: mosaic(reclassedTIFFs, 'mosaic.tif')
def mosaic(inRasterList, outRasterPath):
    codeIn = ['gdalwarp','-t_srs', 'EPSG:5070'] + inRasterList + [outRasterPath]
    process = subprocess.Popen(codeIn,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = process.communicate()
    errcode = process.returncode
    print (out, err)
    # non-zero is error
    return errcode

def translateToGeoTIFF(inRaster, outRasterPath):
    """gdal_translate "D:/SouthFACT_products/latestChange/062320\latestChangeSWIR.tif" "H:\SPA_Secure\Geospatial\SouthFACT\GIS\LatestChange\geotiff\out.tif" -co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co COMPRESS=DEFLATEgdal_translate "D:/SouthFACT_products/latestChange/062320\latestChangeNDVI.tif" "D:/SouthFACT_products/latestChange/062320/out.tif" -co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co COMPRESS=DEFLATE  -co BIGTIFF=YES"""
    codeIn = ['gdal_translate',inRaster,outRasterPath, '-co',  'TILED=YES', '-co', 'COPY_SRC_OVERVIEWS=YES', '-co', 'COMPRESS=DEFLATE', '-co', 'BIGTIFF=YES']
    process = subprocess.Popen(codeIn,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = process.communicate()
    errcode = process.returncode
    print (out, err)
    # non-zero is error
    return errcode

def download(filename, fileId, service):
    # download to disk and remove from drive
    print(filename)    
    request = service.files().get_media(fileId=fileId)
    fh = io.FileIO(os.path.expandvars('%userprofile%/Downloads/' + filename), 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print ("Download %d%%." % int(status.progress() * 100))
    file = service.files().delete(fileId=fileId).execute()

def downloadMultiple(aListOfDicts, service):
    count=0
    # download any available yearly or latest change products
    p = re.compile('(NDVI|SWIR|NDMI)(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(LA|AR|MS|KY|TN|OK|VA|SC|NC|GA|AL|TX|FL|PR|VI)')           
    for d in aListOfDicts:
        i=d['id']
        n=d['name']
        #print(i,n)
        if p.match(n):
            download(n,i, service)
            count += 1
    return count
    
def pendingTasks(ids):
    tasks = ee.data.getTaskList() 
    myTasks = list(filter(lambda x: x['id'] in ids, tasks))     
    myPendingTasks=list(filter(lambda x: x['state'] in ['RUNNING','READY'], myTasks))
    outstandingPendingTasks=len(myPendingTasks)
    print('pending tasks: {0}'.format(outstandingPendingTasks))
    return outstandingPendingTasks
    
def completedTasksRemaining(ids):
    tasks = ee.data.getTaskList() 
    myTasks = list(filter(lambda x: x['id'] in ids, tasks))     
    myCompletedTasks=list(filter(lambda x: x['state'] == 'COMPLETED', myTasks))
    outstandingCompletedTasks=len(myCompletedTasks)
    print('completed tasks: {0}'.format(outstandingCompletedTasks))
    return outstandingCompletedTasks<len(ids)
    
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']
def main():
    """Using the Drive v3 API to download products from GEE for regioanl GeoTIFF.
    """

    downloadDir, outputGeoTIFFDir, ids_file, drive_key_file, credentials_file = parseCmdLine() 
    successfulDownloads = 0 
    creds = None
    #pdb.set_trace()
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(drive_key_file):
        with open(drive_key_file, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(drive_key_file, 'wb') as token:
            pickle.dump(creds, token)
    service = build('drive', 'v3', credentials=creds)
    ee.Initialize()
    text_file = open(ids_file, "r")
    ids = text_file.read().split(',')
    ids = list(filter(None, ids))
    #pdb.set_trace()
    print('Begin download at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S")))
    while True:
        results = service.files().list(pageSize=50, fields="nextPageToken, files(id, name)").execute()
        # get the specifics of the items on drive for download 
        items = results.get('files', [])
        # get any completed tasks and download them
        successfulDownloads += downloadMultiple(items,service)
        print("Waiting for exports to complete.")
        time.sleep(15*60) # Delay for 15 minutes.
        # if there's nothing let to do, quit
        if not (pendingTasks(ids) or completedTasksRemaining(ids) or successfulDownloads < len(ids)):
            break
    # Mainland Southern states mosaic
    print('Begin mosaic to geotiff at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S")))
    os.chdir(downloadDir)
    p = re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(LA|AR|MS|KY|TN|OK|VA|SC|NC|GA|AL|TX|FL)')  
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], downloadDir+'swirLatestChange.tif')
    translateToGeoTIFF(downloadDir+'swirLatestChange.tif', outputGeoTIFFDir+'swirLatestChange.tif')
    # PR|VI SWIR mosaic
    p = re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(PR|VI)')  
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], downloadDir+'swirLatestChange.tif')
    translateToGeoTIFF(downloadDir+'swirLatestChange.tif', outputGeoTIFFDir+'swirLatestChangePRVI.tif')
    p = re.compile('NDMI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(LA|AR|MS|KY|TN|OK|VA|SC|NC|GA|AL|TX|FL)')  
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], downloadDir+'ndmiLatestChange.tif')
    translateToGeoTIFF(downloadDir+'ndmiLatestChange.tif', outputGeoTIFFDir+'ndmiLatestChange.tif')
    # PR|VI mosaic NDMI
    p = re.compile('NDMI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(PR|VI)')  
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], downloadDir+'ndmiLatestChange.tif')
    translateToGeoTIFF(downloadDir+'ndmiLatestChange.tif', outputGeoTIFFDir+'ndmiLatestChange.PRVItif')
    p = re.compile('NDVI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(LA|AR|MS|KY|TN|OK|VA|SC|NC|GA|AL|TX|FL)')  
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], downloadDir+'ndviLatestChange.tif')
    translateToGeoTIFF(downloadDir+'ndviLatestChange.tif', outputGeoTIFFDir+'ndviLatestChange.tif')
    # PR|VI NDVI mosaic
    p = re.compile('NDVI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(PR|VI)')  
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], downloadDir+'ndviLatestChange.tif')
    translateToGeoTIFF(downloadDir+'ndviLatestChange.tif', outputGeoTIFFDir+'ndviLatestChangePRVI.tif')
    print ('Finished at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S")))

if __name__ == '__main__':
    main()