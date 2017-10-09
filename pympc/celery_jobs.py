import helpers.Storage.storage as storage

from celery import Celery, states, Signature, result, chain, group
from celery.result import AsyncResult, GroupResult

from celery_app import qmapp

storageBackend = storage.getStorageBackend()

def getPCFolderDetails(absPath):
	    """ Get the details (count numPoints and extent) of a folder with LAS/LAZ files (using LAStools)"""
    tcount = 0
    (tminx, tminy, tminz, tmaxx, tmaxy, tmaxz) =  (None, None, None, None, None, None)
    (tscalex, tscaley, tscalez) = (None, None, None)

    if storageBackend.isdir(absPath):
        inputFiles = storageBackend.walk(absPath)
    else:
        inputFiles = [absPath,]

    numInputFiles = len(inputFiles)

    tasksQueue = [] # The queue of tasks
    #detailsQueue = multiprocessing.Queue() # The queue of results/details



    for i in range(numInputFiles):
        tasksQueue.append(qmapp.Signature(name="getPCFileDetails", args=(inputFiles[i],)))
    #for i in range(numProc): #we add as many None jobs as numProc to tell them to terminate (queue is FIFO)
    #    tasksQueue.put(None)

    gResults = group(tasksQueue)
    details = res.get()

    for i in range(numInputFiles):
        sys.stdout.write('\r')
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
