#!/usr/bin/python
import argparse, os

from celery import Celery, states, Signature, result, chain, group

from pympc.tasksManagement import qmApp
import helpers.Storage.storage as storage

storageBackend = storage.getStorageBackend()

def run(inputFolder, outputFolder, outputFormat, levels, spacing, extent):
    # Check user parameters
    if not storageBackend.is_dir(inputFolder):
        raise Exception(inputFolder + ' does not exist')

    tasks = []
    for tile in storageBackend.listdir(inputFolder)[0]:
        if tile != 'tiles.js':
        
            tileRelPath = inputFolder + '/' + tile
            outputFolderPath = outputFolder + '/' + tile + '_potree'

            sig = Signature("potreeConverter", args=(tileRelPath, outputFolderPath, outputFormat, levels, spacing, extent,))
            tasks.append(sig)

    job = group(tasks)()
    job.get()
    print("job finished")

def argument_parser():
   # define argument menu
    parser = argparse.ArgumentParser(
    description="Creates a parallel commands XML configuration file. This XML file can be used with pycoeman to run the tasks in a SGE cluster, in a bunch of ssh-reachable hosts or in the local machine")
    parser.add_argument('-i','--input',default='',help='Input folder with the tiles. This folder must contain subfolders, one for each tile. Each tile subfolder must contain the LAS/LAZ files in the tile',type=str, required=True)
    parser.add_argument('-o','--output',default='',help='Output parallel commands XML configuration file',type=str, required=True)
    parser.add_argument('-f','--format',default='',help='Format (LAS or LAZ)',type=str, required=True)
    parser.add_argument('-l','--levels',default='',help='Number of levels for OctTree',type=int, required=True)
    parser.add_argument('-s','--spacing',default='',help='Spacing at root level',type=int, required=True)
    parser.add_argument('-e','--extent',default='',help='Extent to be used for all the OctTree, specify as "minX minY minZ maxX maxY maxZ"',type=str, required=True)
    return parser

def main():
    args = argument_parser().parse_args()
    run(args.input, args.output, args.format, args.levels, args.spacing, args.extent)

if __name__ == "__main__":
    main()
