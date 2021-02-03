from __future__ import print_function
import logging
import boto3
from botocore.exceptions import ClientError
from os import listdir
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
from userConfig import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, downloadDir, outputGeoTIFFDir, ids_file, drive_key_file, credentials_file

def parseCmdLine():
    # Will parse the arguments provided on the command line.
    parser = argparse.ArgumentParser(description='Download GEE products and create regional GeoTIFF')    
    parser.add_argument('-yearly',help="pipeline for yearly change products. Default is for latest change.", action='store_true')
    parser.add_argument('-year',help="in pipeline for yearly change products, the most recent year being processed. e.g., 2019")
    parser.add_argument('-bucket',help="in pipeline for yearly change products, the S3 bucket destination. e.g., 2019-2018/")
    ns = parser.parse_args()
    if ('yearly' in vars(ns) and 'year' not in vars(ns) and 'bucket' not in vars(ns)):
        parser.error('The -yearly argument requires the -year and -bucket arguments')
    return [ns.yearly, ns.year, ns.bucket]

#Upload a file to an S3 bucket
#:param file_name: File to upload
#:param bucket: Bucket to upload to
#:param object_name: S3 object name. If not specified then file_name is used
#:return: True if file was uploaded, else False
def upload_file(file_name, bucket, object_name=None):
 
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# Utility function to mosaic a list of rasters
# @param
#     [inRasterList] - a list of rasters 
#     [outRasterPath] - full path to the output raster in epsg:5070 projection
# @return errcode
def mosaic(inRasterList, outRasterPath):
    codeIn = ['gdalwarp','-t_srs', 'EPSG:5070'] + inRasterList + [outRasterPath]
    process = subprocess.Popen(codeIn,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = process.communicate()
    errcode = process.returncode
    print (out, err)
    # non-zero is error
    return errcode

# Utility function to convert a TIFF to GeoTIFF
# @param
#     [inRaster] - a list of rasters
#     [outRasterPath] - full path to the output GeoTIFF
# @return errcode
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
    fh = io.FileIO(os.path.expandvars(downloadDir + filename), 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print ("Download %d%%." % int(status.progress() * 100))
    file = service.files().delete(fileId=fileId).execute()

def downloadMultiple(aListOfDicts, service):
    downloadedFiles=[]
    # download any available yearly or latest change products
    p = re.compile('(NDVI|SWIR|NDMI)(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}\w*(L8|S2)')           
    for d in aListOfDicts:
        i=d['id']
        n=d['name']
        #print(i,n)
        if p.match(n):
            download(n,i, service)
            downloadedFiles.append(n)
    return downloadedFiles
    
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
    
def mosaicDownloadedToGeotiff(p,mosaicTiffName):
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], downloadDir+mosaicTiffName)
    translateToGeoTIFF(downloadDir+mosaicTiffName, outputGeoTIFFDir+mosaicTiffName)
   
    
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']
def main():
    """Using the Drive v3 API to download products from GEE for regioanl GeoTIFF."""
    yearly, year, bucket = parseCmdLine()   
    if yearly: 
        productName = 'YearlyChange' + year
        bucketName = bucket
    else:
        productName = 'LatestChange'
        bucketName = 'current-year-to-date/'
    successfulDownloads = 0 
    creds = None
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
    tasks = ee.data.getTaskList() 
    myTasks = list(filter(lambda x: x['id'] in ids, tasks))     
    print('Begin download at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S")))
    while True:
        results = service.files().list(pageSize=100, q="mimeType = 'image/tiff'", fields="nextPageToken, files(id, name)").execute()      
        # get the specifics of the items on drive for download 
        items = results.get('files', [])
        # get any completed tasks and download them
        successfulDownloads = len(downloadMultiple(items,service))
        # if there's nothing let to do, quit
        if not (pendingTasks(ids) or completedTasksRemaining(ids) or successfulDownloads):
            break
        else:
            print("Waiting for exports to complete.")
            time.sleep(5*60) # Delay for 5 minutes.
    # find IDs for the scenesBegin and scenesEnd CSVs
    #results = service.files().list(pageSize=100, q="name = 'SWIR-Custom-Change-Between-2020-and-2019scenesBegin.csv' or name = 'SWIR-Custom-Change-Between-2020-and-2019scenesEnd.csv'", fields="nextPageToken, files(id, name)").execute()
    #pdb.set_trace()    
    results = service.files().list(pageSize=100, q="mimeType != 'image/tiff' and name contains 'SWIR-Custom-Change-Between*'", fields="nextPageToken, files(id, name)").execute()
    # get the fileIDs of the items download 
    items = results.get('files', [])
    # get CSVs and download them
    downloadedFiles = downloadMultiple(items,service)
    successfulDownloads=len(downloadedFiles)
    p=re.compile('(NDVI|SWIR|NDMI)(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}\w*(L8|S2)')
    satelliteName = re.search(p,downloadedFiles[0]).group(3)
    if successfulDownloads == 2:
        upload_file(downloadDir+downloadedFiles[0], "data.southfact.com", bucketName+downloadedFiles[0])
        upload_file(downloadDir+downloadedFiles[1], "data.southfact.com", bucketName+downloadedFiles[1])
    # Mainland Southern states and PR VI mosaics
    print('Begin mosaic to geotiff at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S")))
    os.chdir(downloadDir)
    mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)(CONUS|PRVI)'), 'swir' + productName + satelliteName + 'CONUS.tif')
    mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesBegin(L8|S2)CONUS'), 'swirdatesBegin' + productName + satelliteName + 'CONUS.tif')
    mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesEnd(L8|S2)CONUS'), 'swirdatesEnd' + productName + satelliteName + 'CONUS.tif')
    mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)PRVI'), 'swir' + productName + satelliteName + 'PRVI.tif')
    mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesBegin(L8|S2)PRVI'), 'swirdatesBegin' + productName + satelliteName + 'PRVI.tif')
    mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesEnd(L8|S2)PRVI'), 'swirdatesEnd' + productName + satelliteName + 'PRVI.tif')
    if not yearly:
        mosaicDownloadedToGeotiff(re.compile('NDMI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)CONUS'), 'ndmi' + productName + satelliteName + 'CONUS.tif')  
        mosaicDownloadedToGeotiff(re.compile('NDMI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)PRVI'), 'ndmi' + productName + satelliteName + 'PRVI.tif')  
        mosaicDownloadedToGeotiff(re.compile('NDVI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)CONUS'),'ndvi' + productName + satelliteName + 'CONUS.tif')  
        mosaicDownloadedToGeotiff(re.compile('NDVI(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)PRVI'), 'ndvi' + productName + satelliteName + 'PRVI.tif')  
    onlyfiles = [f for f in os.listdir(outputGeoTIFFDir) if isfile(join(outputGeoTIFFDir, f))]    
    for file in onlyfiles:
        upload_file(outputGeoTIFFDir+file, "data.southfact.com", bucketName+file)
    print ('Finished at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S")))
if __name__ == '__main__':
    main()