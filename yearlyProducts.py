import ee, datetime, argparse, pdb
from userConfig import USERNAME

ee.Initialize()

def parseCmdLine():
    # Will parse the arguments provided on the command line.
    parser = argparse.ArgumentParser(description='Generate statewide products for SGSF region.')
    parser.add_argument("startYear", help="string, the current year")
    parser.add_argument('ids_file', help='the fullpath where export to Drive task ids will be stored e.g., G:/productTranscript.txt ')
    parser.add_argument('metadata_ids_file', help='the fullpath where export to Asset task ids will be stored e.g., G:/productTranscript.txt ')
    ns = parser.parse_args()
    return [ns.startYear, ns.ids_file, ns.metadata_ids_file]
def getGeometry(feature):
  # Keep this list of properties.
  keepProperties = ['state_abbr']
  # Get the centroid of the feature's geometry.
  geom = feature.geometry().bounds()
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

  return image.updateMask(mask).divide(10000)

# add band to identify unix time of every pixel used in the composite
def addDateBand(image): 
  #.set('system:time_start', img.get('system:time_start'))
  return image.addBands(image.metadata('system:time_start')).copyProperties(image)
  
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

# used in client-side export of state products
def getGeometry(feature):
  # Keep this list of properties.
  keepProperties = ['state_abbr']
  # Get the centroid of the feature's geometry.
  geom = feature.geometry().bounds()
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

  return image.updateMask(mask).divide(10000)

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
  #https://gis.stackexchange.com/questions/326131/error-in-export-image-from-google-earth-engine-from-python-api
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
 
states = ee.FeatureCollection("users/landsatfact/SGSF_states")
	

startYear, ids_file, metadata_ids_file = parseCmdLine() 
ids_file = open(ids_file, "w")
metadata_ids_file = open(metadata_ids_file, "w")
LANDSAT8 = 'LANDSAT/LC08/C01/T1_SR'
Sentinel2 = 'COPERNICUS/S2'

landsat_sat = 'L8'  # L8 or S2 
beginDay='-11-01'
endDay='-03-31'

secondYear = str(int(startYear)-1)
beginWinter = str(int(secondYear)-1)
endWinter = str(int(startYear)+1)

geometry = states.geometry(1).simplify(1000)

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

# Collect one year of changes over the start and second years, for a date range  
# from the beginning of secondYear winter in secondYear - 1 to end of startYear winter in startYear + 1
collectionRangeStart = ee.ImageCollection(LANDSAT).filter(ee.Filter.date(secondYear + beginDay, startYear + endDay)).map(addDateBand)
collectionRangeEnd = ee.ImageCollection(LANDSAT).filter(ee.Filter.date(startYear + beginDay, endWinter + endDay)).map(addDateBand)
compositeRangeEnd = collectionRangeEnd.map(mask).median()
#pdb.set_trace()
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

exportGeoTiff(SWIRChangeCustomRange, 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear)
exportGeoTiffToAsset(compositeStartYear.select(['system:time_start'], ['observationDate']), 'datesBegin'+ str(startYear))
exportGeoTiffToAsset(compositeSecondYear.select(['system:time_start'], ['observationDate']), 'datesEnd'+ str(startYear))



