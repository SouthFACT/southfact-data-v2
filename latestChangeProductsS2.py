import ee, time, datetime, argparse, pdb

def parseCmdLine():
    # Will parse the arguments provided on the command line.
    # download dir and output regional TIFF full path are mandatory.
    parser = argparse.ArgumentParser(description='Generate GEE latest change products')
    parser.add_argument('ids_file', help='the fullpath where export to Drive task ids will be stored e.g., G:/productTranscript.txt ')
    parser.add_argument('metadata_ids_file', help='the fullpath where export to Asset task ids will be stored e.g., G:/productTranscript.txt ')
    ns = parser.parse_args()
    return [ns.ids_file, ns.metadata_ids_file]
def maskClouds(img) :
  clouds = ee.Image(img.get('cloud_mask')).select('probability')
  isNotCloud = clouds.lt(MAX_CLOUD_PROBABILITY)
  return img.updateMask(isNotCloud)

# The masks for the 10m bands sometimes do not exclude bad data at
# scene edges, so we apply masks from the 20m and 60m bands as well.
# Example asset that needs this operation:
# COPERNICUS/S2_CLOUD_PROBABILITY/20190301T000239_20190301T000238_T55GDP
def maskEdges(s2_img) :
  return s2_img.updateMask(s2_img.select('B8A').mask().updateMask(s2_img.select('B9').mask()))

def maskedS2Collection (filter):
  s2Sr = ee.ImageCollection('COPERNICUS/S2_SR')
  s2Clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
  #Filter input collections by desired data range and region.
  criteria = filter
  s2Sr = s2Sr.filter(criteria).map(maskEdges)
  s2Clouds = s2Clouds.filter(criteria)
  # Join S2 SR with cloud probability dataset to add cloud mask.
  s2SrWithCloudMask = ee.Join.saveFirst('cloud_mask').apply(s2Sr, s2Clouds, ee.Filter.equals(leftField='system:index', rightField='system:index'))
  return ee.ImageCollection(s2SrWithCloudMask).map(maskClouds)
   
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
               'folder': 'users/landsatfact/LatestChangeProducts/',
               'scale':30,
               'maxPixels':1e13,
               'formatOptions': {'cloudOptimized': True}}
    task = ee.batch.Export.image.toDrive(**task_config)
    task.start()
    ids_file.write(',{0}'.format(task.id))
    print ('task ID', task.id, geom['properties']['state_abbr'])
    
# exports image to Google Drive
def exportGeoTiffToAsset(image, exportName):
  geometries=states.map(getGeometry).getInfo().get('features')
  # loop on client side
  for geom in geometries:
    task_config={'image':image,
               'region':ee.Geometry(geometry).intersection(ee.Feature(geom).geometry(),1).bounds().getInfo()['coordinates'],
               'description':exportName+geom['properties']['state_abbr'],
               'scale':30,
               'maxPixels':1e13,
               'assetId': exportName+geom['properties']['state_abbr']}
    task = ee.batch.Export.image.toAsset(**task_config)
    task.start()
    #ids_file.write(', {0}'.format(task.id))
    print ('task ID', task.id, geom['properties']['state_abbr'])


# exports image clear pixel percentage (clear pixel area and total state area in square meters) to Google Drive
def exportCloudTable(image, exportName):
  geometries=states.map(getGeometry).getInfo().get('features')
  bandName = image.bandNames().get(0);
  # loop on client side
  for geom in geometries:
    stateSQM = geom['properties']['sq_miles']*2589988
    npix = image.select(0).multiply(0).add(1).unmask(0).multiply(ee.Image.pixelArea())
    npix=npix.reduceRegion(reducer= ee.Reducer.sum(), geometry= ee.Feature(geom).geometry(), scale= 30, maxPixels= 1e9).get(bandName)
    features = [ee.Feature(None, {'area': npix}),ee.Feature(None, {'area': stateSQM})]  
    task_config={'collection': ee.FeatureCollection(features), 'description': 'clearPercentage'+geom['properties']['state_abbr']}
	#https://gis.stackexchange.com/questions/326131/error-in-export-image-from-google-earth-engine-from-python-api
    task = ee.batch.Export.table.toDrive(**task_config)
    task.start()   

def sceneFeatures(scene):
  return ee.Feature(None, {'value': scene})

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

MAX_CLOUD_PROBABILITY = 65;

# defaults
nir = 'B8'
red = 'B4'
blue = 'B2'
green = 'B3'
swir1 = 'B11'
swir2 = 'B12'
waterThreshold = 0

states = ee.FeatureCollection("users/landsatfact/SGSF_states")
geometry = states.geometry(1).simplify(1000)
# Collect one year of changes over the start and second years, for a date range  
# from the beginning of secondYear winter in secondYear - 1 to end of startYear winter in startYear + 1
# early in the year (before 3/31) use at least 30 days of data 
# e.g., 11/1 to 12/1 on Jan.1 of the current year
# later in the year restrict this range to the end of winter (3/31)
"""
ag_array=collectionRangeStart.aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc.filterBounds(geometry), 'description': 'scenesBegin', 'assetId': 'users/landsatfact/scenesBegin'}	
task = ee.batch.Export.table.toAsset(**task_config)
task.start()

ag_array=collectionRangeEnd.aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc.filterBounds(geometry), 'description': 'scenesEnd', 'assetId': 'users/landsatfact/scenesEnd'}	
task = ee.batch.Export.table.toAsset(**task_config)
task.start()
"""
collectionRangeStart = maskedS2Collection(ee.Filter.And(ee.Filter.date(lastWinterBeginDate,t2.advance(-30,'day')), ee.Filter.date(lastWinterBeginDate, lastWinterEndDate)))
collectionRangeEnd = maskedS2Collection(ee.Filter.date(curentYearBeginDate, endWinterDate))

compositeRangeStart = collectionRangeStart.median()
compositeRangeEnd = collectionRangeEnd.median()

compositeStartYear = compositeRangeEnd # Start Year average - "custom request"

compositeSecondYear = compositeRangeStart # Second Year average - "custom request"

# add the NDWI band to the image so we can get water masks
ndwi = compositeStartYear.normalizedDifference([green, swir1]).rename('NDWI')

# get pixels above the threshold
water = ndwi.lte(waterThreshold)

# update composites with water masks
compositeStartYear = compositeStartYear.updateMask(water)

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
#exportGeoTiffToAsset(compositeStartYear.select(['system:time_start'], ['observationDate']), 'datesBegin'+ str(startYear))
#exportGeoTiffToAsset(compositeSecondYear.select(['system:time_start'], ['observationDate']), 'datesEnd'+ str(startYear))

ids_file.close()