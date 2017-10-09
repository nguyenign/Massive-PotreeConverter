from pywebhdfs.webhdfs import PyWebHdfsClient, errors
import os
hdfsHost = os.getenv("HDFS_NAMENODE", "namenode")
hdfsPort = os.getenv("HDFS_PORT", "50070")
hdfsUser = os.getenv("HDFS_USER", "guest")

hdfs = PyWebHdfsClient(host=hdfsHost,port=hdfsPort, user_name=hdfsUser)

hdfsStorageRoot = os.getenv("HDFS_STORAGEROOT", "/user/" + hdfsUser + "/")