import os
#from flask.helpers import send_file
from tempfile import SpooledTemporaryFile
from pywebhdfs.webhdfs import errors as hdfsErrors
from .hdfsConf import hdfs, hdfsStorageRoot
import errno

def read_from_hdfs(path, **options):
	fullPath = hdfsStorageRoot+path
	fileObj = None
	try:
		fileObj = SpooledTemporaryFile()
		fileObj.write(hdfs.read_file(fullPath))
		fileObj.seek(0)
	except hdfsErrors.Unauthorized:
		raise IOError(errno.EACCES, "unauthorized access", fullPath)
	except hdfsErrors.FileNotFound:
		raise IOError(errno.ENOENT, "file not found", fullPath)
	return fileObj

def write_to_hdfs(hdfsFilePath, localFilePath=None, localFileDescriptor=None, **options):
	if localFilePath and localFileDescriptor:
		raise Exception("write to hdfs: both localFilePath and localFileDescriptor are set. Only one is accepted")

	if (localFilePath==None) and (localFileDescriptor==None):
		raise Exception("write to hdfs: neither localFilePath nor localFileDescriptor are set. One of those must be set")

	fh = None

	try:
		if localFilePath:
			fh = open(localFilePath, "rb")
		if (localFileDescriptor):
			fh = localFileDescriptor
			fh.seek(0)
		hdfs.create_file(hdfsStorageRoot+ hdfsFilePath, fh.read(), overwrite=True)
		fh.close()
	except hdfsErrors.Unauthorized:
		fh.close()
		raise IOError(errno.EACCES, "unauthorized access", hdfsStorageRoot+ hdfsFilePath)

	return True

def list_from_hdfs(hdfsPath):
	fullPath = hdfsStorageRoot+hdfsPath
	r = hdfs.list_dir(fullPath)
	return r

def make_hdfs_dir(hdfsPath):
	return hdfs.make_dir(hdfsPath)