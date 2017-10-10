import utils
import celery
import helpers.Storage.storage as storage
import os
import shutil
from pympc.generate_tiles import getTileIndex, runPDALSplitter

storageBackend = storage.getStorageBackend()

@task(name='getPCFileDetails')
def getPCFileDetails(absPath, removeTmp=True, tmpFile='/tmp/pcFile'):
	storageBackend.get_file(absPath, tmpFile)
	details = utils.getPCFileDetails(tmpFile)
	if removeTmp:
		os.remove(tmpFile)
	return details

@task(name="makeTile")
def generateTilesFromOneFile(inputFile, minX, minY, maxX, maxY, outputFolder, axisTiles, task_id=None):
    # Get number of points and BBOX of this file
    localTmpFolder = "/tmp/" + str(task_id)
    localInputFile = localTmpFolder + "/" + os.basename(inputFile)
    localPdalTmp = localTmpFolder + "/pdal"
    localoutTmp = localTmpFolder = "/out"
    os.makedirs(localTmpFolder)

    (fCount, fMinX, fMinY, _, fMaxX, fMaxY, _, _, _, _, _, _, _) = getPCFileDetails(inputFile, removeTmp=False, tmpFile=localInputFile)
    print ('Processing', os.path.basename(inputFile), fCount, fMinX, fMinY, fMaxX, fMaxY)
    # For the four vertices of the BBOX we get in which tile they should go
    posMinXMinY = getTileIndex(fMinX, fMinY, minX, minY, maxX, maxY, axisTiles)
    posMinXMaxY = getTileIndex(fMinX, fMaxY, minX, minY, maxX, maxY, axisTiles)
    posMaxXMinY = getTileIndex(fMaxX, fMinY, minX, minY, maxX, maxY, axisTiles)
    posMaxXMaxY = getTileIndex(fMaxX, fMaxY, minX, minY, maxX, maxY, axisTiles)

    if (posMinXMinY == posMinXMaxY) and (posMinXMinY == posMaxXMinY) and (posMinXMinY == posMaxXMaxY):
        # If they are the same the whole file can be directly copied to the tile
        tileFolder = outputFolder + '/' + getTileName(*posMinXMinY)
        if not storageBackend.is_dir(tileFolder):
            storageBackend.mkdir(tileFolder)
        storageBackend.send_file(localInputFile, tileFolder + "/" + os.path.basename(inputFile))
    else:
        # If not, we run PDAL gridder to split the file in pieces that can go to the tiles
        tGCount = runPDALSplitter(task_id, localInputFile, localoutTmp, localPdalTmp, minX, minY, maxX, maxY, axisTiles)
        if tGCount != fCount:
            print ('WARNING: split version of ', inputFile, ' does not have same number of points (', tGCount, 'expected', fCount, ')')
        for d in os.listdir(localoutTmp): #d is tilefolder
        	localTileFolder = localoutTmp + "/" + d
        	storageBackend.mkdir(outputFolder + "/" + d)
        	for f in os.listdir(absPath):
        		storageBackend.send_file(os.path.join(localTileFolder, f), os.path.join(outputFolder, d, f))
    shutil.rmtree(localTmpFolder)
    return (inputFile, fCount)
