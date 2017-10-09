import utils
import celery
import helpers.Storage.storage as storage

storageBackend = storage.getStorageBackend()

@task(name='getPCFileDetails')
def getPCFileDetails(absPath):
	tmpFile = '/tmp/pcFile'
	storageBackend.get_file(absPath, tmpFile)
	details = utils.getPCFileDetails(tmpFile)
	return details
