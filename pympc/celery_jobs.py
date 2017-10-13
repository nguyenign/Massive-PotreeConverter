import helpers.Storage.storage as storage

from celery import Celery, states, Signature, result, chain, group
from celery.result import AsyncResult, GroupResult

from pympc.tasksManagement import qmApp
import sys
storageBackend = storage.getStorageBackend()
import math
import os
import json

def getPCFolderDetails(absPath):
    """ Get the details (count numPoints and extent) of a folder with LAS/LAZ files (using LAStools)"""
    tcount = 0
    (tminx, tminy, tminz, tmaxx, tmaxy, tmaxz) =  (None, None, None, None, None, None)
    (tscalex, tscaley, tscalez) = (None, None, None)

    if storageBackend.is_dir(absPath):
        inputFiles = storageBackend.walk(absPath)
    else:
        inputFiles = [absPath,]

    numInputFiles = len(inputFiles)

    tasksQueue = [] # The queue of tasks
    #detailsQueue = multiprocessing.Queue() # The queue of results/details



    for i in range(numInputFiles):
        tasksQueue.append(Signature("getPCFileDetails", args=(inputFiles[i],)))
    #for i in range(numProc): #we add as many None jobs as numProc to tell them to terminate (queue is FIFO)
    #    tasksQueue.put(None)
    print(tasksQueue)
    gResults = group(tasksQueue)()
    details = gResults.get()
    print(details)
    for i in range(numInputFiles):
        sys.stdout.write('\r')
        print(details[i])
        (count, minx, miny, minz, maxx, maxy, maxz, scalex, scaley, scalez, _, _, _) = details[i]
        if i == 0:
            (tscalex, tscaley, tscalez) = (scalex, scaley, scalez)

        tcount += count
        if count:
            if tminx == None or minx < tminx:
                tminx = minx
            if tminy == None or miny < tminy:
                tminy = miny
            if tminz == None or minz < tminz:
                tminz = minz
            if tmaxx == None or maxx > tmaxx:
                tmaxx = maxx
            if tmaxy == None or maxy > tmaxy:
                tmaxy = maxy
            if tmaxz == None or maxz > tmaxz:
                tmaxz = maxz
        sys.stdout.write("\rCompleted %.02f%%" % (100. * float(i) / float(numInputFiles)))
        sys.stdout.flush()
    sys.stdout.write('\r')
    sys.stdout.write('\rCompleted 100.00%!')

    return (inputFiles, tcount, tminx, tminy, tminz, tmaxx, tmaxy, tmaxz, tscalex, tscaley, tscalez)

def generateTiles(inputFolder, outputFolder, tempFolder, extent, numberTiles):
    # Check input parameters
    if not storageBackend.is_dir(inputFolder) and not storageBackend.is_file(inputFolder):
        raise Exception('Error: Input folder does not exist!')
    if storageBackend.is_file(outputFolder):
        raise Exception('Error: There is a file with the same name as the output folder. Please, delete it!')
    elif storageBackend.is_dir(outputFolder) and not storageBackend.dir_is_empty(outputFolder):
        raise Exception('Error: Output folder exists and it is not empty. Please, delete the data in the output folder!')
    # Get the number of tiles per dimension (x and y)
    axisTiles = math.sqrt(numberTiles)
    if (not axisTiles.is_integer()) or (int(axisTiles) % 2):
        raise Exception('Error: Number of tiles must be the square of number which is power of 2!')
    axisTiles = int(axisTiles)

    # Create output and temporal folder
    storageBackend.mkdir(outputFolder)

    (minX, minY, maxX, maxY) = extent.split(' ')
    minX = float(minX)
    minY = float(minY)
    maxX = float(maxX)
    maxY = float(maxY)

    if (maxX - minX) != (maxY - minY):
        raise Exception('Error: Tiling requires that maxX-minX must be equal to maxY-minY!')

    inputFiles = storageBackend.walk(inputFolder)
    numInputFiles = len(inputFiles)
    print ('%s contains %d files' % (inputFolder, numInputFiles))

    # Create queues for the distributed processing
    tasksQueue = []
    #resultsQueue = multiprocessing.Queue() # The queue of results

    # Add tasks/inputFiles
    for i in range(numInputFiles):
        tasksQueue.append(Signature("makeTile", args=(inputFiles[i],minX, minY, maxX, maxY, outputFolder, axisTiles,)))

    job = group(tasksQueue)()
    res = job.get()


    # Get all the results (actually we do not need the returned values)
    numPoints = 0
    for i in range(numInputFiles):
        (inputFile, inputFileNumPoints) = res[i]
        numPoints += inputFileNumPoints
        print ('Completed %d of %d (%.02f%%)' % (i+1, numInputFiles, 100. * float(i+1) / float(numInputFiles)))


    # Write the tile.js file with information about the tiles
    cFile = open("/tmp" + '/tiles.js', 'w')
    d = {}
    d["NumberPoints"] = numPoints
    d["numXTiles"] = axisTiles
    d["numYTiles"] = axisTiles
    d["boundingBox"] = {'lx':minX,'ly':minY,'ux':maxX,'uy':maxY}
    cFile.write(json.dumps(d,indent=4,sort_keys=True))
    cFile.close()

    storageBackend.save_file("/tmp/tiles.js", outputFolder + "/tiles.js")
    os.remove("/tmp/tiles.js")