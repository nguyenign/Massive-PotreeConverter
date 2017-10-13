import os, struct, glob

import numpy as np
class lasHandler():
    def __init__(self, lasPath):
        self.fh = open(lasPath, "rb")
        fh = self.fh
        fh.seek(0)
        ##print fh.tell()


        if self.fh.read(4) != b"LASF":
            raise()
        self.fileSourceID = struct.unpack("<H", fh.read(2))[0]
        self.globalEncoding = struct.unpack("<H", fh.read(2))[0]
        self.GUID1 = struct.unpack("<L", fh.read(4))[0]
        self.GUID2 = struct.unpack("<H", fh.read(2))[0]
        self.GUID3 = struct.unpack("<H", fh.read(2))[0]
        self.GUID4 = fh.read(8)
        self.versionMajor = struct.unpack("<B", fh.read(1))[0]
        self.versionMinor = struct.unpack("<B", fh.read(1))[0]
        self.systemID = fh.read(32)
        self.generatingSoftware = fh.read(32)
        self.creationDayOfYear = struct.unpack("<H", fh.read(2))[0]
        self.creationYear = struct.unpack("<H", fh.read(2))[0]
        self.headerSize = struct.unpack("<H", fh.read(2))[0]
        self.offsetToPoints = struct.unpack("<L", fh.read(4))[0]
        self.numOfVLR = struct.unpack("<L", fh.read(4))[0]
        self.pointDataFormat = struct.unpack("<B", fh.read(1))[0]
        self.pointDataLength = struct.unpack("<H", fh.read(2))[0]
        ##print fh.tell()
        self.legacyNumberOfPointrecords = struct.unpack("<L", fh.read(4))[0]
        
        self.legacyNumberOfPointByReturn = struct.unpack("<LLLLL", fh.read(20))
        
        self.scaleFactor = [struct.unpack("<d", fh.read(8))[0],
                            struct.unpack("<d", fh.read(8))[0],
                            struct.unpack("<d", fh.read(8))[0]]
        self.offset = [struct.unpack("<d", fh.read(8))[0],
                            struct.unpack("<d", fh.read(8))[0],
                            struct.unpack("<d", fh.read(8))[0]]
        maxX = struct.unpack("<d", fh.read(8))[0]
        minX = struct.unpack("<d", fh.read(8))[0]
        maxY = struct.unpack("<d", fh.read(8))[0]
        minY = struct.unpack("<d", fh.read(8))[0]
        maxZ = struct.unpack("<d", fh.read(8))[0]
        minZ = struct.unpack("<d", fh.read(8))[0]
        self.max = [maxX, maxY, maxZ]

        self.numberOfPoints = self.legacyNumberOfPointrecords

        self.min = [minX, minY, minZ]
        if self.versionMajor == 1 and self.versionMinor == 4:
            self.startOfWaveForm = struct.unpack("<Q", fh.read(8))
            self.startOfEVLR = struct.unpack("<Q", fh.read(8))
            self.numberOfEVLR = struct.unpack("<L", fh.read(4))
            self.numberOfPointRecords = struct.unpack("<Q", fh.read(8))            
            self.numberOfPointByReturn = struct.unpack("<QQQQQQQQQQQQQQQ", fh.read(120))

            self.numberOfPoints = self.numberOfPointRecords
        fh.close()

    def getBBoxSize(self):
        return [self.max[0]-self.min[0], self.max[1]-self.min[1], self.max[2]-self.min[2]]

    def getSurface(self):
        box = self.getBBoxSize()
        return box[0]*box[1]
                            
    def __str__(self):
        ret = ""
        for k in dir(self):
            if not k.startswith("__"):
                ret += k + ":" + getattr(self, k).__str__() + "\n"
        return ret
if __name__=="__main__":
    maxPoint = 0
    surface = 0
    files = glob.glob("/media/IGN/LAS/*.las")
    
    lh = lasHandler(files[0])
    
    mmin = lh.min
    mmax = lh.max
    lh.fh.close()
    for i in files:
        lh = lasHandler(i)
        for j in range(0,3):
            mmin[j] = min(mmin[j], lh.min[j])
            mmax[j] = max(mmax[j], lh.max[j])
            
        maxPoint += lh.legacyNumberOfPointrecords
        surface += lh.getSurface()/1000000.0
        lh.fh.close()
    print(maxPoint)
    print(surface)
    
    print(mmin)
    print(mmax)
    """
    
    print lh
    print lh.getBBoxSize()
    lh.fh.close()
    """
