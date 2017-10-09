class AbstractImageDatabase:
	def __init__(self):
		pass

	def getImagePath(self, imageId):
		return None

	def getImageMetadata(self, imageId):
		return None

	def rebuildImageDatabase(self):
		return True