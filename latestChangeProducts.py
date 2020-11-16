import ee, time, datetime, argparse, pdb
from userConfig import USERNAME

def parseCmdLine():
    # Will parse the arguments provided on the command line.
    # download dir and output regional TIFF full path are mandatory.
    parser = argparse.ArgumentParser(description='Generate GEE latest change products')
    parser.add_argument('ids_file', help='the fullpath where export to Drive task ids will be stored e.g., G:/productTranscript.txt ')
    parser.add_argument('metadata_ids_file', help='the fullpath where export to Asset task ids will be stored e.g., G:/productTranscript.txt ')
    ns = parser.parse_args()
    return [ns.ids_file, ns.metadata_ids_file]
    
# reference time.time
# Return the current time in seconds since the Epoch.
# Fractions of a second may be present if the system clock provides them.
# Note: if your system clock provides fractions of a second you can end up 
# with results like: 1405821684785.2 
# conversion to an int prevents this
def now_milliseconds():
   return int(time.time() * 1000)


# used in client-side export of state products
def getGeometry(feature):
  # Keep this list of properties.
  keepProperties = ['state_abbr', 'sq_miles']
  # Get the centroid of the feature's geometry.
  geom = feature.geometry()
  # Return a new Feature, copying properties from the old Feature.
  return ee.Feature(geom).copyProperties(feature, keepProperties)

# /**
# * Function to mask clouds using the Sentinel-2 QA band
# * @param {ee.Image} image Sentinel-2 image
# * @return {ee.Image} cloud masked Sentinel-2 image
# */
def maskS2Clouds(image):
  qa = image.select('QA60')

  # Bits 10 and 11 are clouds and cirrus, respectively.
  cloudBitMask = 1 << 10
  cirrusBitMask = 1 << 11

  # Both flags should be set to zero, indicating clear conditions.
  mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(cirrusBitMask).eq(0))

  return image.updateMask(mask).divide(10000).copyProperties(image)

  
# Function to cloud mask from the pixel_qa band of Landsat 8 SR data.
def maskLandsatClouds(image):
  # Bits 3 and 5 are cloud shadow and cloud, respectively.
  cloudShadowBitMask = 1 << 3
  cloudsBitMask = 1 << 5

  # Get the pixel QA band.
  qa = image.select('pixel_qa')

  # Both flags should be set to zero, indicating clear conditions.
  mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudsBitMask).eq(0))

  # Return the masked image, scaled to TOA reflectance, without the QA bands.
  return image.updateMask(mask).divide (10000).select("B[0-9]*").addBands(qa).addBands(image.select('system:time_start')).copyProperties(image, ['system:time_start'])
      
# add band to identify unix time of every pixel used in the composite
def addDateBand(image): 
  #.set('system:time_start', img.get('system:time_start'))
  return image.addBands(image.metadata('system:time_start')).copyProperties(image)


# use gee normalizedDifference
def normDiff(image, band1, band2):
  return image.normalizedDifference([band1, band2])

# force percent change image into a 0-255 range so it is true 8 bit image
def make255(image, band):
  imageLT127 = image.expression('b1 > 127 ? 127 : b1' ,{'b1': image.select(band)})
  image127 = imageLT127.expression('b1 < -127 ? -127 : b1' ,{'b1': imageLT127.select(band)})
  image255 = image127.add(128)
  return image255.uint8()

# use gee normalizedDifference to get change in two filtered datasets
def normDiffChange(compositeYearOne, compositeYearTwo, band1, band2):
  yearOneNormDiff = compositeYearOne.normalizedDifference([band1, band2]) 
  yearTwoNormDiff = compositeYearTwo.normalizedDifference([band1, band2]) 
  changeYearOneYearTwo = yearTwoNormDiff.subtract(yearOneNormDiff)
  YearTwoABS = yearOneNormDiff.abs()
  YearOneYearTwoDivide = changeYearOneYearTwo.divide(YearTwoABS)
  YearOneYearTwoPercent = YearOneYearTwoDivide.multiply(100)
  band255 = 'nd'
  image255 = make255(YearOneYearTwoPercent, band255)
  return image255

# use one band and get difference (change)
def oneBandDiff(compositeYearOne, compositeYearTwo, band1):
  yearOneImg = compositeYearOne.select([band1])
  yearTwoImg = compositeYearTwo.select([band1])
  changeYearOneYearTwo = yearTwoImg.subtract(yearOneImg)
  yearOneImgABS = yearOneImg.abs()
  YearOneYearTwoDivide = changeYearOneYearTwo.divide(yearOneImgABS)
  YearOneYearTwoPercent = YearOneYearTwoDivide.multiply(100)
  image255 = make255(YearOneYearTwoPercent, band1)
  return image255

# exports image to Google Drive
def exportGeoTiff(image, exportName):
  geometries=states.map(getGeometry).getInfo().get('features')
  # loop on client side
  for geom in geometries:
    task_config={'image':image,
               'region':ee.Geometry(geometry).intersection(ee.Feature(geom).geometry(),1).bounds().getInfo()['coordinates'],
               'description':exportName+geom['properties']['state_abbr'],
               'scale':30,
               'maxPixels':1e13,
               'formatOptions': {'cloudOptimized': True}}
    task = ee.batch.Export.image.toDrive(**task_config)
    task.start()
    ids_file.write(',{0}'.format(task.id))
    print ('task ID', task.id, geom['properties']['state_abbr'])
    
# exports image to Asset
def exportGeoTiffToAsset(image, exportName):
    task_config={'image':image,
               'region':geometry,
               'description':exportName,
               'scale':30,
               'maxPixels':1e13,
               'assetId': 'users/'+USERNAME+'/' + exportName}
    task = ee.batch.Export.image.toAsset(**task_config)
    task.start()
    metadata_ids_file.write(', {0}'.format(task.id))
    print ('task ID', task.id)
    
def sceneFeatures(scene):
  return ee.Feature(geometry, {'value': scene})
  
ee.Initialize()
ids_file, metadata_ids_file = parseCmdLine() 
ids_file = open(ids_file, "w")
metadata_ids_file = open(metadata_ids_file, "w")
mstimeone = now_milliseconds()

#convert to server-side datetime
t2 = ee.Date(mstimeone)
startYear = t2.get('year').getInfo()
t3 = t2.advance(-1, 'year')
secondYear = t3.get('year').getInfo()

t4 = t3.advance(-1, 'year')
beginWinter = t4.get('year')
endWinter =(t2.advance(+1, 'year').get('year'))
endWinterDate=ee.Date.fromYMD(endWinter, 3, 31, 'America/New_York')
lastWinterBeginDate=ee.Date.fromYMD(secondYear, 11, 1, 'America/New_York')
lastWinterEndDate=ee.Date.fromYMD(startYear, 3, 31, 'America/New_York')
curentYearBeginDate=ee.Date.fromYMD(startYear, 1, 1, 'America/New_York')

LANDSAT8 = 'LANDSAT/LC08/C01/T1_SR'
Sentinel2 = 'COPERNICUS/S2'

landsat_sat = 'L8'  # L8 or S2 
# set default bands and cloud mask
if landsat_sat=='L8':
    nir = 'B5'
    red = 'B4'
    blue = 'B2'
    green = 'B3'
    swir1 = 'B6'
    swir2 = 'B7'
    LANDSAT = LANDSAT8
    mask = maskLandsatClouds
    dateID = 'LANDSAT_ID'
elif landsat_sat=='S2':
    nir = 'B8'
    red = 'B4'
    blue = 'B2'
    green = 'B3'
    swir1 = 'B11'
    swir2 = 'B12'
    LANDSAT = Sentinel2
    mask = maskS2Clouds
    dateID = 'DATATAKE_IDENTIFIER'

# defaults
LANDSAT = LANDSAT8
nir = 'B5'
red = 'B4'
blue = 'B2'
green = 'B3'
swir1 = 'B6'
swir2 = 'B7'
waterThreshold = 0
mask = maskLandsatClouds
dateID = 'LANDSAT_ID'

states = ee.FeatureCollection("users/landsatfact/SGSF_states")
geometry = states.geometry().bounds()
# Collect one year of changes over the start and second years, for a date range  
# from the beginning of secondYear winter in secondYear - 1 to end of startYear winter in startYear + 1
# early in the year (before 3/31) use at least 30 days of data 
# e.g., 11/1 to 12/1 on Jan.1 of the current year
# later in the year restrict this range to the end of winter (3/31)
collectionRangeStart = ee.ImageCollection(LANDSAT).filterDate(lastWinterBeginDate,t2.advance(-30,'day')).filterDate(lastWinterBeginDate, lastWinterEndDate).map(addDateBand)
collectionRangeEnd = ee.ImageCollection(LANDSAT).filterDate(curentYearBeginDate, endWinterDate).map(addDateBand)

ag_array=collectionRangeStart.filterBounds(geometry).distinct(dateID).aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc, 'description': 'scenesBegin',
               'assetId': 'users/'+USERNAME+'/scenesBegin'}	
task = ee.batch.Export.table.toAsset(**task_config)
task.start()
metadata_ids_file.write(',{0}'.format(task.id))
ag_array=collectionRangeEnd.filterBounds(geometry).distinct(dateID).aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc, 'description': 'scenesEnd',
               'assetId': 'users/'+USERNAME+'/scenesEnd'}	
task = ee.batch.Export.table.toAsset(**task_config)
task.start()
metadata_ids_file.write(',{0}'.format(task.id))

compositeRangeStart = collectionRangeStart.map(mask).median()
compositeRangeEnd = collectionRangeEnd.map(mask).median()

compositeStartYear = compositeRangeEnd # Start Year average - "custom request"
compositeSecondYear = compositeRangeStart # Second Year average - "custom request"

# add the NDWI band to the image so we can get water masks
ndwi = compositeStartYear.normalizedDifference([green, swir1]).rename('NDWI')

# get pixels above the threshold
water = ndwi.lte(waterThreshold)

# update composites with water masks
compositeStartYear = compositeStartYear.updateMask(water)
compositeSecondYear = compositeSecondYear.updateMask(water)

# make RGB images 
RGBStartYear = compositeStartYear.select(red, green, blue)
RGBSecondYear = compositeSecondYear.select(red, green, blue)

# NDMI change
NDMIChangeCustomRange = normDiffChange(compositeSecondYear, compositeStartYear, nir, swir1)

# NDVI change
NDVIChangeCustomRange = normDiffChange(compositeSecondYear, compositeStartYear, nir, red)

# SWIR change
SWIRChangeCustomRange = oneBandDiff(compositeSecondYear,compositeStartYear, swir2)
                    
#exportGeoTiff(RGBStartYear, 'RGB'+ startYear)
#exportGeoTiff(RGBSecondYear, 'RGB'+ secondYear)

exportGeoTiff(NDVIChangeCustomRange, 'NDVI-Latest-Change-Between-'+str(startYear)+'-and-'+str(secondYear))
exportGeoTiff(SWIRChangeCustomRange, 'SWIR-Latest-Change-Between-'+str(startYear)+'-and-'+str(secondYear))
exportGeoTiff(NDMIChangeCustomRange, 'NDMI-Latest-Change-Between-'+str(startYear)+'-and-'+str(secondYear))

#export datetimes used for pixel values in one year of changes over the start and second years
exportGeoTiffToAsset(compositeStartYear.select(['system:time_start'], ['observationDate']), 'datesBegin'+ str(startYear))
exportGeoTiffToAsset(compositeSecondYear.select(['system:time_start'], ['observationDate']), 'datesEnd'+ str(startYear))

ids_file.close()
metadata_ids_file.close()