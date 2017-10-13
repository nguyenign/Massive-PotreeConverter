#!/usr/bin/env python
"""
This script is used to distribute the points of a bunch of LAS/LAZ files in
different tiles. The XY extent of the different tiles match the XY extent of the
nodes of a certain level of a octree defined by the provided bounding box (z
is not required by the XY tiling). Which level of the octree is matched
depends on specified number of tiles:
 - 4 (2X2) means matching with level 1 of octree
 - 16 (4x4) means matching with level 2 of octree
 and so on.
"""

import argparse, traceback, time, os, math, multiprocessing, json
from pympc import utils
from pympc import celery_jobs

def argument_parser():
    """ Define the arguments and return the parser object"""
    parser = argparse.ArgumentParser(
    description="""This script is used to distribute the points of a bunch of LAS/LAZ files in
different tiles. The XY extent of the different tiles match the XY extent of the
nodes of a certain level of a octree defined by the provided bounding box (z
is not required by the XY tiling). Which level of the octree is matched
depends on specified number of tiles:
 - 4 (2X2) means matching with level 1 of octree
 - 16 (4x4) means matching with level 2 of octree
 and so on. """)
    parser.add_argument('-i','--input',default='',help='Input data folder (with LAS/LAZ files)',type=str, required=True)
    parser.add_argument('-o','--output',default='',help='Output data folder for the different tiles',type=str, required=True)
    parser.add_argument('-t','--temp',default='',help='Temporal folder where required processing is done',type=str, required=True)
    parser.add_argument('-e','--extent',default='',help='XY extent to be used for the tiling, specify as "minX minY maxX maxY". maxX-minX must be equal to maxY-minY. This is required to have a good extent matching with the octree',type=str, required=True)
    parser.add_argument('-n','--number',default='',help='Number of tiles (must be the power of 4. Example: 4, 16, 64, 256, 1024, etc.)',type=int, required=True)
    return parser

def main():
    args = argument_parser().parse_args()
    print ('Input folder: ', args.input)
    print ('Output folder: ', args.output)
    print ('Temporal folder: ', args.temp)
    print ('Extent: ', args.extent)
    print ('Number of tiles: ', args.number)

    try:
        t0 = time.time()
        print ('Starting ' + os.path.basename(__file__) + '...')
        celery_jobs.generateTiles(args.input, args.output, args.temp, args.extent, args.number)
        print( 'Finished in %.2f seconds' % (time.time() - t0))
    except:
        print ('Execution failed!')
        print( traceback.format_exc())

if __name__ == "__main__":
    main()
