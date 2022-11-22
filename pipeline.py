from __future__ import print_function
import logging, boto3, pickle, io, argparse, contextlib, json, re, threading, time, uuid, ee, subprocess, datetime, os, pdb, pathlib, shutil, collections
from botocore.exceptions import ClientError
from os import listdir
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
from osgeo import gdal
from os.path import isfile, join
from pathlib import Path
from userConfig import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, downloadDir, outputGeoTIFFDir, ids_file, drive_key_file, credentials_file, geeService_account, geeServiceAccountCredentials
from oauth2client.service_account import ServiceAccountCredentials
from contextlib import redirect_stdout
outputDir = outputGeoTIFFDir
# Will parse the arguments provided on the command line.
def parseCmdLine():

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
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL': 'public-read'})
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

    #pdb.set_trace()
    gdal.SetConfigOption('CHECK_DISK_FREE_SPACE', 'FALSE')
    gdal.Warp(outRasterPath, inRasterList, options= '-of COG' + " -overwrite -multi -wm 1296 -t_srs EPSG:5070 -co TILED=YES -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS -co COPY_SRC_OVERVIEWS=YES")
#

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

#download a file from Drive
def download(filename, fileId, service):
    # download to disk and remove from drive
    print(filename)    
    request = service.files().get_media(fileId=fileId)
    fh = io.FileIO(os.path.expandvars(downloadDir + filename), 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        with open('/home/ec2-user/GitHub/southfact-data-v2/debug.log', 'a') as f:
            print("Download %d%%." % int(status.progress() * 100), file=f, flush=True)
        print ("Download %d%%." % int(status.progress() * 100), flush=True)
    file = service.files().delete(fileId=fileId).execute()

# download any available yearly or latest change products
def downloadMultiple(aListOfDicts, service):
    downloadedFiles=[]
    p = re.compile('(NDVI.?|SWIR.?|NDMI.?)(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}\w*(L8|S2)')           
    for d in aListOfDicts:
        i=d['id']
        n=d['name']
        #print(i,n)
        if p.match(n):
            download(n,i, service)
            downloadedFiles.append(n)
    return downloadedFiles
    
#answer the number of pending tasks in GEE     
def pendingTasks(ids):
    tasks = ee.data.getTaskList() 
    myTasks = list(filter(lambda x: x['id'] in ids, tasks))     
    myPendingTasks=list(filter(lambda x: x['state'] in ['RUNNING','READY'], myTasks))
    outstandingPendingTasks=len(myPendingTasks)
    with open('/home/ec2-user/GitHub/southfact-data-v2/debug.log', 'a') as f:
        print('pending tasks: {0}'.format(outstandingPendingTasks), file=f, flush=True)
    print('pending tasks: {0}'.format(outstandingPendingTasks), flush=True)
    return outstandingPendingTasks

#answer the number of tasks remaining in the pipeline           
def completedTasksRemaining(ids):
    tasks = ee.data.getTaskList() 
    myTasks = list(filter(lambda x: x['id'] in ids, tasks))     
    myCompletedTasks=list(filter(lambda x: x['state'] == 'COMPLETED', myTasks))
    outstandingCompletedTasks=len(myCompletedTasks)
    with open('/home/ec2-user/GitHub/southfact-data-v2/debug.log', 'a') as f:
        print('completed tasks: {0}'.format(outstandingCompletedTasks), file=f, flush=True)
    print('completed tasks: {0}'.format(outstandingCompletedTasks), flush=True)
     
    return outstandingCompletedTasks<len(ids)

#create regionwide GeoTIFF
def mosaicDownloadedToGeotiff(p,mosaicTiffName):
    print('GeoTIFFs input: {0}'.format(downloadDir+mosaicTiffName))
    print('GeoTIFFs output: {0}'.format(outputDir+mosaicTiffName))
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]    
    mosaic([s for s in onlyfiles if p.match(s)], outputDir+mosaicTiffName)
    #translateToGeoTIFF(downloadDir+mosaicTiffName, outputDir+mosaicTiffName)

#create change poly shapefiles for the states   
def polygonize(inRaster, outShapePath):
    tempfile = downloadDir+'/changePolys.tiff'

    print("Begin polygonize at ", datetime.datetime.now())
    with rasterio.Env():
        with rasterio.open(inRaster, 'r') as src:
            with rasterio.open(tempfile, 'w', **src.profile) as dst:
                for ij, window in src.block_windows():
                    array = src.read(window=window)
                    array=array.astype(dtype=np.int16, copy=False)
                    array -= 186
                    result = np.clip(array, 0,1)
                    dst.write(result.astype(dtype=np.uint8, copy=False), window=window)
   
        with rasterio.open(tempfile) as src:
            image = src.read(1) # first band
            print("Convert to shapes ", datetime.datetime.now())
            results = (
            {'properties': {'raster_val': v}, 'geometry': s, 'area': Polygon(s['coordinates'][0]).area}
            for i, (s, v)
            # assumes 
                in enumerate(
                shapes(image, mask=None, transform=src.transform))if Polygon(s['coordinates'][0]).area > 80936 and v >0)
            polys=list(results)

    print("Begin shapefile creation ", datetime.datetime.now())
    # transform polygons from WGS84 to Albers Equal Area Conic
    schemaNew={"geometry": 'Polygon', "properties": OrderedDict({'raster_val': 'float', 'area': 'float'})}
    #schemaNew={"geometry": 'Polygon', "properties": OrderedDict({'raster_val': 'float'})}


    with fiona.open(outShapePath, "w", driver="ESRI Shapefile",crs=from_epsg(5070), schema=schemaNew) as c:
        #wgs84 = pyproj.CRS('EPSG:5070')
        #utm = pyproj.CRS('EPSG:32618')
        #project = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform
        for feature in polys:
            #print("Feature: ", feature)
            poly=Polygon(feature['geometry']['coordinates'][0])
            c.write({
                 'geometry': mapping(poly),
                 'properties': {'raster_val': feature['properties']['raster_val'], 'area' : feature['area']}
    })          

#convert individual states yeary change to GeoTIFF
def downloadedToShape(p,rasterName):
    onlyfiles = [f for f in os.listdir(downloadDir) if isfile(join(downloadDir, f))]   
    gen  = (s for s in onlyfiles if p.match(s))    
    for tiffName in gen:
        stateName = re.search(p,downloadedFiles[0]).group(2)
        baseName = rasterName+stateName
        translateToGeoTIFF(downloadDir+tiffName, downloadDir+baseName+ '.tif')
        polygonize(downloadDir+baseName+ '.tif',outputDir+baseName+ '.shp')


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']
def main():

    try:
        """Using the Drive v3 API to download products from GEE for upload to S3."""
        yearly, year, bucket = parseCmdLine()   
        if yearly: 
            productName = 'YearlyChange' + year
            bucketName = bucket
            csvString = "\'SWIR-Custom-Change-Between-*\'"

        else:
            productName = 'LatestChange'
            bucketName = 'current-year-to-date/'
            csvString = "\'SWIR-Latest-Change-Between-*\'"
        successfulDownloads = 0 
        creds = None
        #pdb.set_trace() 

        credentials = ee.ServiceAccountCredentials(geeService_account,geeServiceAccountCredentials)
        collections.Callable = collections.abc.Callable
        ee.Initialize(credentials)
        driveCredentials=ServiceAccountCredentials.from_json_keyfile_name(geeServiceAccountCredentials, scopes=SCOPES)
        service = build('drive', 'v3', credentials=driveCredentials)
        #ee.Initialize()
        text_file = open(ids_file, "r")
        ids = text_file.read().split(',')
        ids = list(filter(None, ids))
        tasks = ee.data.getTaskList() 
        #print('ids ',ids)
        tasks = ee.data.getTaskList()
        
        #print('tasks2 ', tasks)
        myTasks = list(filter(lambda x: x['id'] in ids, tasks))     
        print('Begin download at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S")))
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
                with open('/home/ec2-user/GitHub/southfact-data-v2/debug.log', 'a') as f:
                    print("Waiting for exports to complete.", file=f, flush=True)
                print("Waiting for exports to complete.", flush=True)
                time.sleep(5*60) # Delay for 5 minutes.
        # find IDs for the scenesBegin and scenesEnd CSVs
        queryStr="mimeType != 'image/tiff' and name contains "+csvString  
        results = service.files().list(pageSize=100, q=queryStr, fields="nextPageToken, files(id, name)").execute()
        # get the fileIDs of the items download 
        items = results.get('files', [])
        # get CSVs and download them
        downloadedFiles = downloadMultiple(items,service)
        successfulDownloads=len(downloadedFiles)
        #pdb.set_trace()  
        p=re.compile('(NDVI.?|SWIR.?|NDMI.?)(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}\w*(L8|S2)')
        satelliteName = re.search(p,downloadedFiles[0]).group(3)
        if successfulDownloads == 2:
            upload_file(downloadDir+downloadedFiles[0], "data.southfact.com", bucketName+downloadedFiles[0])
            upload_file(downloadDir+downloadedFiles[1], "data.southfact.com", bucketName+downloadedFiles[1])
        # Mainland Southern states and PR VI mosaics
        print('Begin mosaic to geotiff at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S")))
        os.chdir(downloadDir)
        # no metadata for now
        #mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesBegin(L8|S2)CONUS'), 'swirdatesBegin' + productName + satelliteName + 'CONUS.tif')
        #mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesEnd(L8|S2)CONUS'), 'swirdatesEnd' + productName + satelliteName + 'CONUS.tif')
        #mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesBegin(L8|S2)PRVI'), 'swirdatesBegin' + productName + satelliteName + 'PRVI.tif')
        #mosaicDownloadedToGeotiff(re.compile('SWIR(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}datesEnd(L8|S2)PRVI'), 'swirdatesEnd' + productName + satelliteName + 'PRVI.tif')
        if yearly:
            # for yearly statewide products produce a shapefile of change polys and a regional GeoTIFF
            #pdb.set_trace()
            mosaicDownloadedToGeotiff(re.compile('SWIR.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)CONUS'), 'swir' + productName + satelliteName + productName + satelliteName + 'CONUS.tif')
            mosaicDownloadedToGeotiff(re.compile('SWIR.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)PRVI'), 'swir' + productName + satelliteName + 'PRVI.tif')
            # no shapefile of change polys  for now            
            #downloadedToShape(re.compile('SWIR.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(LA|AR|MS|KY|TN|OK|VA|SC|NC|GA|AL|TX|FL|PR|VI)(L8|S2)', 'swir' + productName + satelliteName))
        else:
            mosaicDownloadedToGeotiff(re.compile('SWIR.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)CONUS'), 'swir' + productName + satelliteName + 'CONUS.tif')
            mosaicDownloadedToGeotiff(re.compile('SWIR.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)PRVI'), 'swir' + productName + satelliteName + 'PRVI.tif')
            mosaicDownloadedToGeotiff(re.compile('NDMI.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)CONUS'), 'ndmi' + productName + satelliteName + 'CONUS.tif')  
            mosaicDownloadedToGeotiff(re.compile('NDMI.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)PRVI'), 'ndmi' + productName + satelliteName + 'PRVI.tif')  
            mosaicDownloadedToGeotiff(re.compile('NDVI.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)CONUS'),'ndvi' + productName + satelliteName + 'CONUS.tif')  
            mosaicDownloadedToGeotiff(re.compile('NDVI.?(-Latest|-Custom)-Change-Between-[0-9]{4}-and-[0-9]{4}(L8|S2)PRVI'), 'ndvi' + productName + satelliteName + 'PRVI.tif')  
        onlyfiles = [f for f in os.listdir(outputDir) if isfile(join(outputDir, f))]    
        for file in onlyfiles:
            upload_file(outputDir+file, "data.southfact.com", bucketName+file)
    except Exception as e: print(e)
    finally:
        #clean up my mess
        shutil.rmtree('/mnt/efs/fs1/GeoTIFF', ignore_errors=True)  
        shutil.rmtree('/mnt/efs/fs1/output', ignore_errors=True)  
        print ('Finished at {0}'.format(datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S")))


if __name__ == '__main__':
    main()
