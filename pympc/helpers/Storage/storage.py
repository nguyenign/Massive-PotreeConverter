import os
from .hdfsConf import hdfs, hdfsStorageRoot
from .hdfs import * 

storageBackend = os.getenv("STORAGE_BACKEND", "local")
from functools import partial

class LocalStorage():
	name = "local"

	def __init__(self):
		self.rootFolder = os.getenv("STORAGE_ROOTPATH", '/tmp/')

	def save_file(self, filePath, destFilePath):		
		fd = open(filePath, 'rb')
		self.save_file_handler(fd, destFilePath).close()		


	def save_file_handler(self, fileHandler, destFilePath):
		try:
			destFilePath = self.rootFolder + destFilePath
			fileDirectory = os.path.dirname(destFilePath)
			if fileDirectory != "":
				if not os.path.exists(fileDirectory):
					os.makedirs(fileDirectory)
			fdest = open(destFilePath, "wb")
			fileHandler.seek(0)
			fdest.write(fileHandler.read())
			fdest.close()
		except:
			raise("problem saving the file")
		return fileHandler

	def is_dir(self, path):
		return os.path.isdir(self.rootFolder + path)

	def is_file(self, path):
		return os.path.isfile(self.rootFolder + path)

	def mkdir(self, path):
		if not os.path.exists(self.rootFolder + path):
			try:
				os.makedirs(self.rootFolder + path)
			except:
				pass

	def dir_is_empty(self, path):
	    d,f = self.listdir(path)
	    if len(d)==0 and len(f)==0:
	        return True
	    else:
	        return False

	def read_file(self, filePath, mode="rb"):
		filePath = self.rootFolder + filePath
		return open(filePath, mode)

	def get_file(self, destFilePath, filePath, start=0, length=-1):
		fd = self.read_file(destFilePath)
		fh = open(filePath, "wb")
		fd.seek(start,0)
		
		eaten=0		
		chunkSize = 1024
		eat = True

		while eat:
			if (eaten + chunkSize > length) and (length > -1):
				chunkSize = length - eaten
			buf = fd.read(chunkSize)
			fh.write(buf)
			eaten += chunkSize
			if eaten == length and length != -1:
				eat = False
			else:
				eat = buf

		fd.close()
		fh.close()

		return True

	def listdir(self, path):
		items = os.listdir(self.rootFolder + path)
		subDirs = []
		files = []
		for item in items:
			fp = os.path.join(self.rootFolder + path, item)
			if os.path.isdir(fp):
				subDirs.append(item)
			if os.path.isfile(fp):
				files.append(item)
		return subDirs, files

    # in this function, do not append the root path as we are not dealing with
    # os function. everything should be relative to rootFolder
	def walk(self, rootPath, maxDepth=-1):
		currentDepth = 0
		subFolderToVisit = [rootPath]
		filesPaths = []
		while len(subFolderToVisit) > 0:
			newSubFolderToVisit = []
			for folderToVisit in subFolderToVisit:
				subDirs, files = self.listdir(folderToVisit)
				if (maxDepth == -1) or (currentDepth < maxDepth):
					for subDir in subDirs:
						sd = os.path.join(folderToVisit, subDir)
						newSubFolderToVisit.append(sd)
					for file in files:
						filesPaths.append(os.path.join(folderToVisit, file))
					currentDepth +=1
			subFolderToVisit = newSubFolderToVisit
		return filesPaths


class HDFSStorage(LocalStorage):
	name = "hdfs"
	def save_file_handler(self, fileHandler, destFilePath):
		write_to_hdfs(destFilePath, localFileDescriptor=fileHandler)
		return fileHandler

	def read_file(self, filePath, mode="rb"):
		fd = read_from_hdfs(filePath)
		return fd

	def is_dir(self, path):
		## nooo ! to do change that !
		return True

	def is_file(self, path):
		## nooo ! to do change that !
		return True

	def mkdir(self, path):
		return make_hdfs_dir(path)

	def listdir(self, path):
		res = list_from_hdfs(path)
		items = res["FileStatuses"]["FileStatus"]
		subDirs = []
		files = []
		
		for item in items:
			fp = item["pathSuffix"]
			if item["type"] == "DIRECTORY":
				subDirs.append(fp)
			if item["type"] == "FILE":
				files.append(fp)
		return subDirs, files

def getStorageBackend():
	storage = LocalStorage()
	print(storageBackend)
	if storageBackend == "hdfs":
		print("storage: hdfs")
		storage = HDFSStorage()
	else:
		print("not hdfs !")
	return storage



class dbFileStorage():
	def __init__(self):
		pass

class uploadFileStorage():
	def __init__(self):
		pass