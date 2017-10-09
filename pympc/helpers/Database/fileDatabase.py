import os
from helpers.Storage.storage import getStorageBackend
from abstractDatabase import AbstractImageDatabase
import tempfile
storage = getStorageBackend()

class FileImageDatabase(AbstractImageDatabase):
	def __init__(self):
		self.loadDBFile()

	def loadDBFile(self):
		self.databaseFilePath = os.getenv("DATABASE_FILE_PATH", "database.txt")
		try:
			fd = storage.read_file(self.databaseFilePath)
			lines = fd.readlines()
			fd.close()
			self.db = {}
			for line in lines:
				ll = line.split()
				self.db[ll[0]] = ll[1]
		except IOError as e:
			print "WARNING: database not found. initializing an empty db"
			self.db = {}


	def getImagePath(self, imageId):
		if imageId in self.db:
			return self.db[imageId]
		return None

	def rebuildImageDatabase(self):
		imageStorageRoot = os.getenv("IMAGE_STORAGE_ROOT", "imageDB")
		allImages = storage.walk(imageStorageRoot)
		print allImages
		fh = tempfile.SpooledTemporaryFile(mode="w")
		for imagePath in allImages:
			id = os.path.splitext(os.path.basename(imagePath))[0]
			fh.write("%s %s\n" % (id, imagePath))
		fh.seek(0)
		storage.save_file_handler(fh, self.databaseFilePath)
		fh.close()
		self.loadDBFile()
		return True

