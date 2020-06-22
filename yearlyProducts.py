import ee, datetime, argparse
ee.Initialize()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Generate statewide products for SGSF region. 
    """)
    parser.add_argument("startYear", help="string, the current year")

    args = parser.parse_args()

    startYear = args.startYear

sgsf = ee.FeatureCollection("users/landsatfact/SGSF")
states = ee.FeatureCollection("users/landsatfact/SGSF_states")
	
dataset = ee.ImageCollection('USDA/NAIP/DOQQ').filter(ee.Filter.date('2016-01-01', '2018-12-31'))
trueColor = dataset.select(['R', 'G', 'B'])
trueColorVis = { 
  'min': 0.0,
  'max': 255.0,
}

LANDSAT8 = 'LANDSAT/LC08/C01/T1_SR'
Sentinel2 = 'COPERNICUS/S2'

landsat_sat = 'L8'  # L8 or S2 
beginDay='-11-01'
endDay='-03-31'

secondYear = str(int(startYear)-1)
beginWinter = str(int(secondYear)-1)
endWinter = str(int(startYear)+1)

geometry = states.geometry(1).simplify(1000)
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
  return image.updateMask(mask).divide (10000).select("B[0-9]*").addBands(qa).copyProperties(image, ["system:time_start"])
      
# used to identify cloudy nd clear pixels in raw (unmasked) scenes
def addCloudBand(image):
  # Bits 3 and 5 are cloud shadow and cloud, respectively.
  cloudShadowBitMask = 1 << 3
  cloudsBitMask = 1 << 5

  # Get the pixel QA band.
  qa = image.select('pixel_qa')
  mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudsBitMask).eq(0))
  return image.addBands(mask.select(['pixel_qa'],['clouds']))

# supply cloud masked percentage of scenes used
def exportStats (clouds, geom):
  masked=clouds.select('clouds').Not()
  unmasked=clouds.mask()
  
  # Sum the area of clear pixels (unmasked in cloud band, equals 1)
  geometries=states.map(getGeometry).getInfo().get('features')
  #geometries=states.filter(ee.Filter.eq('name', 'North Carolina')).map(getGeometry).getInfo().get('features')
  # loop on client side
  for g in geometries:
    #args={'collection': ee.FeatureCollection(ee.Geometry(g['geometry'])),'reducer': ee.Reducer.sum(),'scale': 30, 'tileScale': 8}
    areaImage = masked.multiply(ee.Image.pixelArea())
    # Sum the area of cloudy pixels (masked in cloud band not, equals 1)
    statsCloudy = areaImage.reduceRegions(ee.FeatureCollection(ee.Geometry(g['geometry'])),ee.Reducer.sum(),30)
    areaImage = unmasked.multiply(ee.Image.pixelArea())
    # Sum the area of clear pixels (unmasked in cloud band, equals 1)
    statsClear = areaImage.reduceRegions(ee.FeatureCollection(ee.Geometry(g['geometry'])),ee.Reducer.sum(),30)

    #print('stats', statsCloudy.merge(statsClear))
    prefix='clouds'
    results = ee.FeatureCollection(statsCloudy.merge(statsClear))
    tag = g['properties']['state_abbr']
    task=ee.batch.Export.table.toDrive(results, prefix+tag)
    task.start()


# for the entire imagecollection, get the pct of remaining cloud masked pixels 
def buildCloudPct(cloudCollection):
  #var cloudCollection = collection.select('clouds')
  cloudMin = cloudCollection.max()
  exportStats(cloudCollection.max(), cloudCollection.geometry())


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
elif landsat_sat=='S2':
    nir = 'B8'
    red = 'B4'
    blue = 'B2'
    green = 'B3'
    swir1 = 'B11'
    swir2 = 'B12'
    LANDSAT = Sentinel2
    mask = maskS2Clouds
 
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

# Collect one year of changes over the start and second years, for a date range  
# from the beginning of secondYear winter in secondYear - 1 to end of startYear winter in startYear + 1
collectionRangeStart = ee.ImageCollection(LANDSAT).filter(ee.Filter.date(secondYear + beginDay, startYear + endDay))
#print('collectionRangeStart', collectionRangeStart.limit(50))

#buildCloudPct(collectionRangeStart.map(addCloudBand).select('clouds'))
collectionRangeEnd = ee.ImageCollection(LANDSAT).filter(ee.Filter.date(startYear + beginDay, endWinter + endDay))

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

NDMI_SLD = ('<RasterSymbolizer>' 
                        '<ColorMap>' 
                            '<ColorMapEntry color="#d62f27" label="0" opacity="1.0" quantity="0"/>' 
                            '<ColorMapEntry color="#e05138" label="7" opacity="1.0" quantity="7"/>' 
                            '<ColorMapEntry color="#eb6f4d" label="27" opacity="1.0" quantity="27"/>' 
                            '<ColorMapEntry color="#f28e63" label="47" opacity="1.0" quantity="47"/>' 
                            '<ColorMapEntry color="#faaf7d" label="67" opacity="1.0" quantity="67"/>' 
                            '<ColorMapEntry color="#fcce95" label="87" opacity="1.0" quantity="87"/>' 
                            '<ColorMapEntry color="#fff0b3" label="107" opacity="1.0" quantity="107"/>' 
                            '<ColorMapEntry color="#f0f2bf" label="127" opacity="1.0" quantity="127"/>' 
                            '<ColorMapEntry color="#d3dbbf" label="147" opacity="1.0" quantity="147"/>' 
                            '<ColorMapEntry color="#b7c4bd" label="167" opacity="1.0" quantity="167"/>' 
                            '<ColorMapEntry color="#9bafbd" label="187" opacity="1.0" quantity="187"/>' 
                            '<ColorMapEntry color="#7f9aba" label="207" opacity="1.0" quantity="207"/>' 
                            '<ColorMapEntry color="#6388b8" label="227" opacity="1.0" quantity="227"/>' 
                            '<ColorMapEntry color="#4575b5" label="247" opacity="1.0" quantity="247"/>' 
                        '</ColorMap>' 
                    '</RasterSymbolizer>')
                    
NDVI_SLD = ('<RasterSymbolizer>' 
                        '<ColorMap>' 
                            '<ColorMapEntry color="#ff2200" label="0" opacity="1.0" quantity="0"/>' 
                            '<ColorMapEntry color="#ff5100" label="7" opacity="1.0" quantity="7"/>' 
                            '<ColorMapEntry color="#ff7300" label="27" opacity="1.0" quantity="27"/>' 
                            '<ColorMapEntry color="#ff9100" label="47" opacity="1.0" quantity="47"/>' 
                            '<ColorMapEntry color="#ffb300" label="67" opacity="1.0" quantity="67"/>' 
                            '<ColorMapEntry color="#ffd000" label="87" opacity="1.0" quantity="87"/>' 
                            '<ColorMapEntry color="#fff200" label="107" opacity="1.0" quantity="107"/>' 
                            '<ColorMapEntry color="#e8f000" label="127" opacity="1.0" quantity="127"/>' 
                            '<ColorMapEntry color="#bdd600" label="147" opacity="1.0" quantity="147"/>' 
                            '<ColorMapEntry color="#97bd00" label="167" opacity="1.0" quantity="167"/>' 
                            '<ColorMapEntry color="#70a300" label="187" opacity="1.0" quantity="187"/>' 
                            '<ColorMapEntry color="#4d8c00" label="207" opacity="1.0" quantity="207"/>' 
                            '<ColorMapEntry color="#2d7500" label="227" opacity="1.0" quantity="227"/>' 
                            '<ColorMapEntry color="#006100" label="247" opacity="1.0" quantity="247"/>' 
                        '</ColorMap>' 
                    '</RasterSymbolizer>')

SWIR_SLD = ('<RasterSymbolizer>' 
                        '<ColorMap>' 
                            '<ColorMapEntry color="#00003c" label="0" opacity="1.0" quantity="0"/>' 
                            '<ColorMapEntry color="#000059" label="7" opacity="1.0" quantity="7"/>' 
                            '<ColorMapEntry color="#000080" label="27" opacity="1.0" quantity="27"/>' 
                            '<ColorMapEntry color="#0000e8" label="47" opacity="1.0" quantity="47"/>' 
                            '<ColorMapEntry color="#0080ff" label="67" opacity="1.0" quantity="67"/>' 
                            '<ColorMapEntry color="#00c080" label="87" opacity="1.0" quantity="87"/>' 
                            '<ColorMapEntry color="#01ff00" label="107" opacity="1.0" quantity="107"/>' 
                            '<ColorMapEntry color="#c0ff00" label="127" opacity="1.0" quantity="127"/>' 
                            '<ColorMapEntry color="#ffff00" label="147" opacity="1.0" quantity="147"/>' 
                            '<ColorMapEntry color="#ffc100" label="167" opacity="1.0" quantity="167"/>' 
                            '<ColorMapEntry color="#ff8000" label="187" opacity="1.0" quantity="187"/>' 
                            '<ColorMapEntry color="#ff0000" label="207" opacity="1.0" quantity="207"/>' 
                            '<ColorMapEntry color="#a30000" label="227" opacity="1.0" quantity="227"/>' 
                            '<ColorMapEntry color="#590004" label="247" opacity="1.0" quantity="247"/>' 
                        '</ColorMap>' 
                    '</RasterSymbolizer>')
                    

                    
SWIR_THRESHOLD_SLD = ('<RasterSymbolizer>' 
                        '<ColorMap>' 
                            '<ColorMapEntry color="#ff8000" label="186" opacity="0.0" quantity="186"/>' 
                            '<ColorMapEntry color="#ff8000" label="187" opacity="1.0" quantity="187"/>' 
                            '<ColorMapEntry color="#ff0000" label="207" opacity="1.0" quantity="207"/>' 
                            '<ColorMapEntry color="#a30000" label="227" opacity="1.0" quantity="227"/>' 
                            '<ColorMapEntry color="#590004" label="247" opacity="1.0" quantity="247"/>' 
                        '</ColorMap>' 
                    '</RasterSymbolizer>')


#exportGeoTiff(RGBStartYear, 'RGB'+ startYear)
#exportGeoTiff(RGBSecondYear, 'RGB'+ secondYear)

#exportGeoTiff(NDVIChangeCustomRange, 'NDVI-Custom-Range-Change-Between-'+startYear+'-and-'+secondYear)

#exportGeoTiff(SWIRChangeCustomRange.sldStyle(SWIR_THRESHOLD_SLD), 'SWIR-Custom-Range-Change-Between-'+startYear+'-and-'+secondYear)
exportGeoTiff(SWIRChangeCustomRange, 'SWIR-Custom-Range-Change-Between-'+startYear+'-and-'+secondYear)


#exportGeoTiff(NDMIChangeCustomRange, 'NDMI-Custom-Range-Change-Between-'+startYear+'-and-'+secondYear)

