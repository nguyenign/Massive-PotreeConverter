import os
from abstractDatabase import *

__databaseBackend = os.getenv("DBBACKEND", "file")
__imageDatabase = AbstractImageDatabase()

if __databaseBackend == "file":
	from fileDatabase import FileImageDatabase
	__imageDatabase = FileImageDatabase()

def getDBImageBackend():
	return __imageDatabase