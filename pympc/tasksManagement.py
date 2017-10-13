from celery import Celery
import os


brokerURL = os.getenv("BROKER_URL", "rabbitmq")
brokerUser = os.getenv("BROKER_USER", "guest")
brokerPassw = os.getenv("BROKER_PASSW", "guest")
backendURL = os.getenv("BACKEND_URL", "redis://redis:6379/0")

qmApp = Celery('tasks', 
	backend=backendURL, 
	broker='pyamqp://'+ brokerUser + ":" + brokerPassw + "@" + brokerURL+'//',
	include=['pympc.celery_tasks'])

qmApp.conf.task_routes = {'*': {'queue': 'potreeConverter'}}

qmApp.conf.task_send_sent_event = True
qmApp.conf.worker_send_task_events = True

if __name__ == '__main__':
    qmApp.start()