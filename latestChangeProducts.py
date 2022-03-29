import ee, time, datetime, argparse, pdb, pathlib, shutil
from userConfig import ids_file

def parseCmdLine():
    # Will parse the arguments provided on the command line.
    parser = argparse.ArgumentParser(description='Generate GEE latest change products.')
    parser.add_argument('-S2',help="Sentinel-2 latest change products. Default is for Landsat 8 change.", action='store_true')    
    ns = parser.parse_args()
    return ns.S2
    
# reference time.time
# Return the current time in seconds since the Epoch.
# Fractions of a second may be present if the system clock provides them.
# Note: if your system clock provides fractions of a second you can end up 
# with results like: 1405821684785.2 
# conversion to an int prevents this
def now_milliseconds():
   return int(time.time() * 1000)
  
def maskS2CloudsQA60(image):
  qa = image.select('QA60')

  # Bits 10 and 11 are clouds and cirrus, respectively.
  cloudBitMask = 1 << 10
  cirrusBitMask = 1 << 11

  # Both flags should be set to zero, indicating clear conditions.
  mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(cirrusBitMask).eq(0))
  return image.updateMask(mask)

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

def exportRegionGeoTiff(image, indexName,startYear, secondYear):						 
  task_config={'image':image,
               'region':conus1,
               'description':indexName+'-Latest-Change-Between-'+startYear+'-and-'+secondYear+productName+'CONUS',
               'scale':30,
               'maxPixels':1e13,
               'shardSize':256,
               'fileDimensions': [75264,55808],
               'formatOptions': {'cloudOptimized': True}}
  task = ee.batch.Export.image.toDrive(**task_config)
  task.start()
  ids_file.write(',{0}'.format(task.id))
																
  task_config={'image':image,
               'region':conus2,
               'description':indexName+'1'+'-Latest-Change-Between-'+startYear+'-and-'+secondYear+productName+'CONUS',
               'scale':30,
               'maxPixels':1e13,
               'shardSize':256,
               'fileDimensions': [75264,55808],
               'formatOptions': {'cloudOptimized': True}}
  task = ee.batch.Export.image.toDrive(**task_config)
  task.start()
  ids_file.write(',{0}'.format(task.id))
																	
  task_config={'image':image,
               'region':prvi,
               'description':indexName+'-Latest-Change-Between-'+startYear+'-and-'+secondYear+productName+'PRVI',
               'scale':30,
               'maxPixels':1e13,
               'shardSize':256,
               'fileDimensions': [75264,55808],
               'formatOptions': {'cloudOptimized': True}}
  task = ee.batch.Export.image.toDrive(**task_config)
  task.start()
  ids_file.write(',{0}'.format(task.id))
 
def sceneFeatures(scene):
  return ee.Feature(geometry, {'value': scene})
    
# create my workspaces, but no complaints if they are already there
path = pathlib.Path('/mnt/efs/fs1/GeoTIFF')    
path.mkdir(parents=True, exist_ok=True)
path = pathlib.Path('/mnt/efs/fs1/output')    
path.mkdir(parents=True, exist_ok=True)
  
ee.Initialize()
states = ee.FeatureCollection("users/landsatfact/SGSF_states")
prvi = states.filter(ee.Filter.Or(ee.Filter.eq('state_abbr', 'PR'),(ee.Filter.eq('state_abbr', 'VI')))).geometry().bounds();
conus1 = states.filter(ee.Filter.Or(ee.Filter.eq('state_abbr', 'KY'),(ee.Filter.eq('state_abbr', 'VA')),
                          (ee.Filter.eq('state_abbr', 'TN')),(ee.Filter.eq('state_abbr', 'NC')),
                          (ee.Filter.eq('state_abbr', 'MS')),(ee.Filter.eq('state_abbr', 'AL')),
                          (ee.Filter.eq('state_abbr', 'GA')),(ee.Filter.eq('state_abbr', 'SC')),
                          (ee.Filter.eq('state_abbr', 'FL')))).geometry().bounds();
conus2 = states.filter(ee.Filter.Or(ee.Filter.eq('state_abbr', 'OK'),(ee.Filter.eq('state_abbr', 'TX')),
                          (ee.Filter.eq('state_abbr', 'AR')),(ee.Filter.eq('state_abbr', 'LA')))).geometry().bounds();S2 = parseCmdLine() 
ids_file = open(ids_file, "w")
mstimeone = now_milliseconds()

#convert to server-side datetime
t2 = ee.Date(mstimeone)
startYear = t2.get('year').getInfo()
t3 = t2.advance(-1, 'year')
secondYear = t3.get('year').getInfo()

t4 = t3.advance(-1, 'year')
beginWinter = t4.get('year')
endWinter =(t2.advance(+1, 'year').get('year'))
endDate=t2
lastWinterBeginDate=ee.Date.fromYMD(secondYear, 11, 1, 'America/New_York')
lastWinterEndDate=ee.Date.fromYMD(startYear, 3, 31, 'America/New_York')
curentYearBeginDate=ee.Date.fromYMD(startYear, 1, 1, 'America/New_York')
waterThreshold = 0
geometry = states.geometry().bounds()
# set default bands and cloud mask
if S2:
    nir = 'B8'
    red = 'B4'
    blue = 'B2'
    green = 'B3'
    swir1 = 'B11'
    swir2 = 'B12'
    SATELLITE = 'COPERNICUS/S2_SR'
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

states = ee.FeatureCollection("users/landsatfact/SGSF_states")
# Collect one year of changes over the start and second years, for a date range  
# from the beginning of secondYear winter in secondYear - 1 to end of startYear winter in startYear + 1
# early in the year (before 3/31) use at least 30 days of data 
# e.g., 11/1 to 12/1 on Jan.1 of the current year
# shouuld we, later in the year, restrict this range to the end of winter (3/31)
# currently, after April, we begin to see greenup changes. 
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
collectionRangeStart = ee.ImageCollection(SATELLITE).filterDate(lastWinterBeginDate,t2.advance(-30,'day')).filterDate(lastWinterBeginDate, lastWinterEndDate).map(addDateBand)
# debuging
collectionRangeEnd = ee.ImageCollection(SATELLITE).filterDate('2021-11-01', '2021-12-31').map(addDateBand)
#collectionRangeEnd = ee.ImageCollection(SATELLITE).filterDate(curentYearBeginDate, endDate).map(addDateBand)

ag_array=collectionRangeStart.filterBounds(geometry).distinct(dateID).aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc, 'description': 'SWIR-Latest-Change-Between-'+str(startYear)+'-and-'+str(secondYear)+'scenesBegin'+productName}	
task = ee.batch.Export.table.toDrive(**task_config)
task.start()
ids_file.write(',{0}'.format(task.id))
ag_array=collectionRangeEnd.filterBounds(geometry).distinct(dateID).aggregate_array(dateID)
fc = ee.FeatureCollection(ag_array.map(sceneFeatures))
task_config={'collection': fc, 'description': 'SWIR-Latest-Change-Between-'+str(startYear)+'-and-'+str(secondYear)+'scenesEnd'+productName}	
task = ee.batch.Export.table.toDrive(**task_config)
task.start()
ids_file.write(',{0}'.format(task.id))

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

exportRegionGeoTiff(NDVIChangeCustomRange,'NDVI',str(startYear),str(secondYear))
exportRegionGeoTiff(SWIRChangeCustomRange,'SWIR',str(startYear),str(secondYear))
exportRegionGeoTiff(NDMIChangeCustomRange,'NDMI',str(startYear),str(secondYear))

#export datetimes used for pixel values in one year of changes over the start and second years. Ignore metadata for now due to speed and Drive space concerns
#exportRegionGeoTiff(compositeStartYear.select(['system:time_start'], ['observationDate']), 'SWIR-Latest-Change-Between-'+str(startYear)+'-and-'+str(secondYear)+'datesBegin')
#exportRegionGeoTiff(compositeSecondYear.select(['system:time_start'], ['observationDate']), 'SWIR-Latest-Change-Between-'+str(startYear)+'-and-'+str(secondYear)+'datesEnd')

ids_file.close()
