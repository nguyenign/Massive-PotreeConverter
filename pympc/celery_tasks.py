import utils
import celery
from celery import task
import helpers.Storage.storage as storage
import os
import shutil
from pympc.generate_tiles import getTileIndex, getTileName, runPDALSplitter
from pympc.utils import shellExecute

storageBackend = storage.getStorageBackend()

@task(name='getPCFileDetails', bind=True)
def getPCFileDetails(self, absPath, removeTmp=True, ):

    base, ext = os.path.splitext(absPath)
    tmpFile = "/tmp/" + str(self.request.id) + ext
    storageBackend.get_file(destFilePath=absPath, filePath=tmpFile, length=4096)

    details = utils.getPCFileDetails(tmpFile)
    print(details)
    if removeTmp:
        os.remove(tmpFile)
    return details

@task(name="makeTile", bind=True)
def generateTilesFromOneFile(self, inputFile, minX, minY, maxX, maxY, outputFolder, axisTiles):
    task_id = self.request.id
    # Get number of points and BBOX of this file
    localTmpFolder = "/tmp/" + str(task_id)
    try:
        localInputFile = localTmpFolder + "/" + os.path.basename(inputFile)
        localPdalTmp = localTmpFolder + "/pdal"
        localoutTmp = localTmpFolder + "/out"
        print(localTmpFolder)
        os.makedirs(localTmpFolder)

        storageBackend.get_file(destFilePath=inputFile, filePath=localInputFile)

        (fCount, fMinX, fMinY, _, fMaxX, fMaxY, _, _, _, _, _, _, _) =  utils.getPCFileDetails(localInputFile)
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
            print("%s -> %s"  %(localInputFile, tileFolder + "/" + os.path.basename(inputFile)))
            storageBackend.save_file(filePath=localInputFile, destFilePath=tileFolder + "/" + os.path.basename(inputFile))
        else:

            # If not, we run PDAL gridder to split the file in pieces that can go to the tiles
            tGCount = runPDALSplitter(task_id, localInputFile, localoutTmp, localPdalTmp, minX, minY, maxX, maxY, axisTiles)
            if tGCount != fCount:
                print ('WARNING: split version of ', inputFile, ' does not have same number of points (', tGCount, 'expected', fCount, ')')
            for d in os.listdir(localoutTmp): #d is tilefolder
                localTileFolder = localoutTmp + "/" + d
                storageBackend.mkdir(outputFolder + "/" + d)
                for f in os.listdir(localTileFolder):

                    localPath = os.path.join(localTileFolder, f)
                    remotePath = os.path.join(outputFolder, d, f)
                    print("%s -> %s"  %(localPath, remotePath))
                    storageBackend.save_file(localPath, remotePath)
            
    except:
        print("error tiling")        
    shutil.rmtree(localTmpFolder)
    return (inputFile, fCount)

@task(name="potreeConverter", bind=True)
def potreeConverter(self, tileFolderPath, outputFolderPath, outputFormat, levels, spacing, extent):
    task_id = self.request.id
    localTmpFolder = "/tmp/" + str(task_id)
    try:
        localFolderPath = localTmpFolder + "/" + tileFolderPath    
        localOutFolderPath = localTmpFolder + "/" + outputFolderPath
        
        os.makedirs(localFolderPath)
        # create an output directory (tile_potree)
        os.makedirs(localOutFolderPath)
        
        # get the tileFolderPath locally
        
        for tileFile in storageBackend.listdir(tileFolderPath)[1]:
            storageBackend.get_file(destFilePath=os.path.join(tileFolderPath, tileFile), filePath=os.path.join(localFolderPath, tileFile))


        # start potreeConverter --source localTileFolderPath --outdir localOutDir
        # PotreeConverter is started in the directory containing the tiles subfolders
        out = shellExecute('/opt/PotreeConverter/build/PotreeConverter/PotreeConverter '+  
                        ' --outdir ' + localOutFolderPath + 
                        ' --levels ' + str(levels) + 
                        ' --output-format ' + str(outputFormat).upper() + 
                        ' --source ' + localFolderPath + 
                        ' --spacing ' + str(spacing) + 
                        ' --aabb "' + extent + '"')
        print("Sending %s -> %s" % (localOutFolderPath, outputFolderPath))
        storageBackend.save_dir(localOutFolderPath, outputFolderPath)    
    except Exception as e:
        print(e)           # __str__ allows args to be printed directly
    finally:
        shutil.rmtree(localTmpFolder)
