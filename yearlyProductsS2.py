import ee, datetime, argparse, pdb
ee.Initialize()
sgsf = ee.FeatureCollection("users/landsatfact/SGSF")
states = ee.FeatureCollection("users/landsatfact/SGSF_states")
s2Sr = ee.ImageCollection('COPERNICUS/S2_SR');
s2Clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')


def parseCmdLine():
    # Will parse the arguments provided on the command line.
    parser = argparse.ArgumentParser(description='Generate statewide products for SGSF region.')
    parser.add_argument("startYear", help="string, the current year")
    parser.add_argument('ids_file', help='the fullpath where export to Drive task ids will be stored e.g., G:/productTranscript.txt ')
    parser.add_argument('metadata_ids_file', help='the fullpath where export to Asset task ids will be stored e.g., G:/productTranscript.txt ')
    ns = parser.parse_args()
    return [ns.startYear, ns.ids_file, ns.metadata_ids_file]

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

def maskedS2Collection (startDate, endDate):
  s2Sr = ee.ImageCollection('COPERNICUS/S2_SR')
  s2Clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
  #Filter input collections by desired data range and region.
  criteria = ee.Filter.date(startDate, endDate)
  s2Sr = s2Sr.filter(criteria).map(maskEdges)
  s2Clouds = s2Clouds.filter(criteria)
  # Join S2 SR with cloud probability dataset to add cloud mask.
  s2SrWithCloudMask = ee.Join.saveFirst('cloud_mask').apply(s2Sr, s2Clouds, ee.Filter.equals(leftField='system:index', rightField='system:index'))
  return ee.ImageCollection(s2SrWithCloudMask).map(maskClouds)

geometry = states.geometry(1).simplify(1000)
# used in client-side export of state products
def getGeometry(feature):
  # Keep this list of properties.
  keepProperties = ['state_abbr']
  # Get the centroid of the feature's geometry.
  geom = feature.geometry().bounds()
  # Return a new Feature, copying properties from the old Feature.
  return ee.Feature(geom).copyProperties(feature, keepProperties)

# used to identify cloudy nd clear pixels in raw (unmasked) scenes
def addCloudBand(image):
  # Bits 3 and 5 are cloud shadow and cloud, respectively.
  cloudShadowBitMask = 1 << 3
  cloudsBitMask = 1 << 5
  # Get the pixel QA band.
  qa = image.select('pixel_qa')
  mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudsBitMask).eq(0))
  return image.addBands(mask.select(['pixel_qa'],['clouds']))
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
  #print(states.aggregate_array('state_abbr'))
  geometries=states.map(getGeometry).getInfo().get('features')
  #geometries=states.filter(ee.Filter.eq('name', 'North Carolina')).map(getGeometry).getInfo().get('features')
  # loop on client side
  #https://gis.stackexchange.com/questions/326131/error-in-export-image-from-google-earth-engine-from-python-api
  for geom in geometries:
    print ('geometries', geom['properties']['state_abbr'])
    task_config={'image':image,
               'region':ee.Geometry(geometry).intersection(ee.Feature(geom).geometry(),1).bounds().getInfo()['coordinates'],
               'description':exportName+geom['properties']['state_abbr'],
               'scale':30,
               'maxPixels':1e13,
               'formatOptions': {'cloudOptimized': True}}
    task = ee.batch.Export.image.toDrive(**task_config)
    task.start()
    ids_file.write(',{0}'.format(task.id))
    
startYear, ids_file, metadata_ids_file = parseCmdLine() 
ids_file = open(ids_file, "w")
metadata_ids_file = open(metadata_ids_file, "w")

beginDay='-11-01'
endDay='-03-31'
secondYear = str(int(startYear)-1)
beginWinter = str(int(secondYear)-1)
endWinter = str(int(startYear)+1)
firstPeriodBegin = secondYear + beginDay
firstPeriodEnd = startYear + endDay
secondPeriodBegin = startYear + beginDay
secondPeriodEnd = endWinter + endDay
MAX_CLOUD_PROBABILITY = 65;

# defaults
nir = 'B8'
red = 'B4'
blue = 'B2'
green = 'B3'
swir1 = 'B11'
swir2 = 'B12'
waterThreshold = 0

# Collect one year of changes over the start and second years, for a date range  
# from the beginning of secondYear winter in secondYear - 1 to end of startYear winter in startYear + 1
#pdb.set_trace()
collectionRangeStart = maskedS2Collection(secondYear + beginDay, startYear + endDay)
collectionRangeEnd = maskedS2Collection(startYear + beginDay, endWinter + endDay)

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
compositeSecondYear = compositeSecondYear.updateMask(water)

# SWIR change
SWIRChangeCustomRange = oneBandDiff(compositeSecondYear,compositeStartYear, swir2)
exportGeoTiff(SWIRChangeCustomRange, 'SWIR-Custom-Change-Between-'+startYear+'-and-'+secondYear)
ids_file.close()


