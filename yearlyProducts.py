import ee, datetime, argparse, pdb, collections
from userConfig import ids_file
service_account = 'aws-southfact-product-generati@awsproductgeneration.iam.gserviceaccount.com'
credentials = ee.ServiceAccountCredentials(service_account, '../keys/awsproductgeneration-8463a020b9aa.json')
collections.Callable = collections.abc.Callable
ee.Initialize(credentials)


def parseCmdLine():
    # Will parse the arguments provided on the command line.
    parser = argparse.ArgumentParser(description='Generate statewide products for SGSF region.')
    parser.add_argument("startYear", help="string, the current year")
    parser.add_argument('-S2',help="Sentinel-2 yearly change products. Default is for Landsat 8 change.", action='store_true')    
    ns = parser.parse_args()
    return [ns.S2, ns.startYear]

# add band to identify unix time of every pixel used in the composite
def addDateBand(image): 
  #.set('system:time_start', img.get('system:time_start'))
  return image.addBands(image.metadata('system:time_start')).copyProperties(image)

def maskS2CloudsQA60(image):
  qa = image.select('QA60')

  # Bits 10 and 11 are clouds and cirrus, respectively.
  cloudBitMask = 1 << 10
  cirrusBitMask = 1 << 11

  # Both flags should be set to zero, indicating clear conditions.
  mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(cirrusBitMask).eq(0))
  return image.updateMask(mask).divide(10000)

  return image.updateMask(mask)
 
# /**
# * Function to mask clouds using the Sentinel-2 QA band
# * @param {ee.Image} image Sentinel-2 image
# * @return {ee.Image} cloud masked Sentinel-2 image
# */
def maskS2Clouds(img) :
  clouds = ee.Image(img.get('cloud_mask')).select('probability')
  isNotCloud = clouds.lt(65)
  return img.updateMask(isNotCloud)

def maskEdges(s2_img) :
  image=s2_img.updateMask(s2_img.select('B8A').mask().updateMask(s2_img.select('B9').mask()))
  return image.addBands(s2_img.select('system:time_start')).copyProperties(s2_img, ['system:time_start'])

def maskedS2Collection (startDate, endDate):
  s2Sr = ee.ImageCollection('COPERNICUS/S2_SR')
  s2Clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
  #Filter input collections by desired data range and region.
  criteria = ee.Filter.date(startDate, endDate)
  #s2Sr = s2Sr.filter(criteria).map(maskEdges)
  s2Clouds = s2Clouds.filter(criteria)
  # Join S2 SR with cloud probability dataset to add cloud mask.
  s2SrWithCloudMask = ee.Join.saveFirst('cloud_mask').apply(s2Sr, s2Clouds, ee.Filter.equals(leftField='system:index', rightField='system:index'))
  return ee.ImageCollection(s2SrWithCloudMask).map(maskS2Clouds)
  
# Applies scaling factors.
def applyScaleFactors(image) :
  opticalBands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
  thermalBands = image.select('ST_B.*').multiply(0.00341802).add(149.0)
  return image.addBands(opticalBands, None, True).addBands(thermalBands, None, True)

 
# Function to cloud mask from the pixel_qa band of Landsat 8 SR data.
def maskLandsatClouds(image):
  cloudShadowBitMask = ee.Number(2).pow(4).int();
  cloudsBitMask = ee.Number(2).pow(3).int();
  cirrusBitMask = ee.Number(2).pow(2).int();
  dilatedCloudsBitMask = ee.Number(2).pow(1).int();
  # Get the pixel QA band.
  qa = image.select('QA_PIXEL');

  # Both flags should be set to zero, indicating clear conditions.
  mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudsBitMask).eq(0))
  # Scale to surface reflectance.
  image = applyScaleFactors(image)
  # Return the masked image, scaled to surface reflectance, without the QA bands.
  return image.updateMask(mask).select("SR_B[0-9]*").addBands(qa).addBands(image.select('system:time_start')).copyProperties(image, ['system:time_start'])
      
# add band to identify unix time of every pixel used in the composite
def addDateBand(image): 
  #.set('system:time_start', img.get('system:time_start'))
  return image.addBands(image.metadata('system:time_start')).copyProperties(image)
  
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
def getGeometry(feature):
  # Keep this list of properties.
  keepProperties = ['state_abbr']
  # Get the centroid of the feature's geometry.
  geom = feature.geometry().bounds()
  # Return a new Feature, copying properties from the old Feature.
  return ee.Feature(geom).copyProperties(feature, keepProperties)
 
def addArea(feature) :
    return feature.set('area', feature.geometry().area(1));


def createStateShapes(feature, image) : 
    # create the changeShapes for each grid cell
    changeShapes= image.gt(186).reduceToVectors(
        reducer = ee.Reducer.countEvery(),
        geometry = feature.geometry(1),
        scale = 30,
        maxPixels = 1e13,
        tileScale = 16,
        eightConnected = False); 
    changeShapes = changeShapes.map(addArea);
    largeChanges = changeShapes.filter(ee.Filter.gt('area', 40468));
    return largeChanges.getInfo();


# exports to Google Drive
def exportSHP(image, exportName,productName):
  geometries=states.map(getGeometry).getInfo().get('features')
  # loop on client side
  #https://gis.stackexchange.com/questions/236707/how-can-i-export-a-set-of-images-from-google-earth-engine/237065#237065
  for geom in geometries:
    feature=ee.Feature(geom);
    grids=(feature.geometry(1).coveringGrid(feature.geometry(1).projection(),250000));
    abbr = feature.get('state_abbr').getInfo();
    region = ee.Geometry(geometry).intersection(feature.geometry(),1).bounds();
    print(abbr+' geometry', region.getInfo());
    #https://gis.stackexchange.com/questions/326131/error-in-export-image-from-google-earth-engine-from-python-api
    fc =grids.map(createStateShapes(feature, image))
    fc =fc.flatten()
    task_config={'collection': fc, 'description': 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear+'shapes'+productName}
    task = ee.batch.Export.image.toDrive(**task_config)
    task.start()
    ids_file.write(',{0}'.format(task.id))

# exports image to Google Drive for a larger area than states
def exportRegionGeoTiff(image, exportName, productName):
  conus = states.filter(ee.Filter.Or(ee.Filter.eq('state_abbr', 'PR'),(ee.Filter.eq('state_abbr', 'VI'))).Not()).geometry().bounds();
  prvi = states.filter(ee.Filter.Or(ee.Filter.eq('state_abbr', 'PR'),(ee.Filter.eq('state_abbr', 'VI')))).geometry().bounds();
  task_config={'image':image.clipToCollection(boundary),
               'region':conus,
               'description':exportName+productName+'CONUS',
               'scale':30,
               'maxPixels':1e13,
               'fileDimensions': [35840,56320],
               'formatOptions': {'cloudOptimized': True}}
  task = ee.batch.Export.image.toDrive(**task_config)
  task.start()
  ids_file.write(',{0}'.format(task.id))
  task_config={'image':image.clipToCollection(boundary),
               'region':prvi,
               'description':exportName+productName+'PRVI',
               'scale':30,
               'maxPixels':1e13,
#               'shardSize': 10*256,
               'fileDimensions': [35840,56320],
               'formatOptions': {'cloudOptimized': True}}
  task = ee.batch.Export.image.toDrive(**task_config)
  task.start()
  ids_file.write(',{0}'.format(task.id))
 


def sceneFeatures(scene):
  return ee.Feature(geometry, {'value': scene})
 
boundary = ee.FeatureCollection("users/landsatfact/clipSGSFCONUS")
states = ee.FeatureCollection("users/landsatfact/SGSF_states")
water = ee.Image("users/landsatfact/sgsfWaterMask")
S2, startYear = parseCmdLine() 
pdb.set_trace()
ids_file = open(ids_file, "w+")
LANDSAT8 = 'LANDSAT/LC08/C02/T1_L2'
Sentinel2 = 'COPERNICUS/S2_SR_HARMONIZED'
beginDay='-11-01'
endDay='-03-31'
secondYear = str(int(startYear)-1)
beginWinter = str(int(secondYear)-1)
endWinter = str(int(startYear)+1)
#waterThreshold = 0
geometry = states.geometry().bounds()

# set default bands and cloud mask
if S2:
    nir = 'B8'
    red = 'B4'
    blue = 'B2'
    green = 'B3'
    swir1 = 'B11'
    swir2 = 'B12'
    SATELLITE = 'COPERNICUS/S2_SR_HARMONIZED'
    mask = maskS2CloudsQA60
    dateID = 'DATATAKE_IDENTIFIER'
    productName = 'S2'
else:
    nir = 'SR_B5'
    red = 'SR_B4'
    blue = 'SR_B2'
    green = 'SR_B3'
    swir1 = 'SR_B6'
    swir2 = 'SR_B7'
    SATELLITE = 'LANDSAT/LC08/C02/T1_L2'
    mask = maskLandsatClouds
    dateID = 'LANDSAT_SCENE_ID'
    productName = 'L8'
#pdb.set_trace()
# Collect one year of changes over the start and second years, for a date range  
# from the beginning of secondYear winter in secondYear - 1 to end of startYear winter in startYear + 1

#for cloud masking with S2_CLOUD_PROBABILITY which is roughly 4 times slower
'''
if S2:
    collectionRangeStart = maskedS2Collection(secondYear + beginDay, startYear + endDay).map(addDateBand)
    collectionRangeEnd = maskedS2Collection(startYear + beginDay, endWinter + endDay).map(addDateBand)
    compositeRangeStart = collectionRangeStart.median()
    compositeRangeEnd = collectionRangeEnd.median()
else:'''
collectionRangeStart = ee.ImageCollection(SATELLITE).filter(ee.Filter.date(secondYear + beginDay, startYear + endDay)).map(addDateBand)
collectionRangeEnd = ee.ImageCollection(SATELLITE).filter(ee.Filter.date(startYear + beginDay, endWinter + endDay)).map(addDateBand)
compositeRangeStart = collectionRangeStart.map(mask).median()
compositeRangeEnd = collectionRangeEnd.map(mask).median()

compositeStartYear = compositeRangeEnd # Start Year average - "custom request"
compositeSecondYear = compositeRangeStart # Second Year average - "custom request"

# add the NDWI band to the image so we can get water masks
#ndwi = compositeStartYear.normalizedDifference([green, swir1]).rename('NDWI')

# update composites with water masks
compositeStartYear = compositeStartYear.updateMask(water.select('b1').neq(1))
compositeSecondYear = compositeSecondYear.updateMask(water.select('b1').neq(1))

# SWIR change
SWIRChangeCustomRange = oneBandDiff(compositeSecondYear,compositeStartYear, swir2)

# First export the set of imagery sources for each pixel comparison used in building SWIRChangeCustomRange
#exportRegionGeoTiff(compositeStartYear.select(['system:time_start'], ['observationDate']), 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear+'datesBegin',productName)
#exportRegionGeoTiff(compositeSecondYear.select(['system:time_start'], ['observationDate']), 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear+'datesEnd',productName)
# Then the set of scenes used in building the images
ag_array=collectionRangeStart.filterBounds(geometry).distinct(dateID).aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc, 'description': 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear+'scenesBegin'+productName}	
task = ee.batch.Export.table.toDrive(**task_config)
task.start()
ids_file.write(',{0}'.format(task.id))
ag_array=collectionRangeEnd.filterBounds(geometry).distinct(dateID).aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc, 'description': 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear+'scenesEnd'+productName}	
task = ee.batch.Export.table.toDrive(**task_config)
task.start()
ids_file.write(',{0}'.format(task.id))
#Lastly export the yearly product
exportRegionGeoTiff(SWIRChangeCustomRange, 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear,productName)
#exportSHP(SWIRChangeCustomRange, 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear,productName)