#!/usr/bin/env python2.7
# Audrey Musselman-Brown

"""
First draft of spark "hello world" toil script 
"""

import sys
from toil.job import Job
from subprocess import call, check_output


# - The tachyon and spark containers need the following ports open:
#   spark master: 7077, 8080, 4040
#   spark worker: 7075
#   tachyon master: 19998, 19999
#   tachyon worker: 29998, 29999

# JOB FUNCTIONS

def initialize(job, numWorkers):

    job.addChildJobFn(spark_master, numWorkers)
    job.addChildJobFn(tachyon_master, numWorkers)

def spark_master(job, numWorkers):

    masterIP = job.addService(SparkMasterService())
    job.addChildJobFn(spark_worker, numWorkers, masterIP)


def spark_workers(job, numWorkers, masterIP):

    for i in range(len(numWorkers)):
        job.addService(SparkWorkerService(masterIP))

    job.addChildJobFn(hello_world)


def tachyon_master(job, numWorkers):

    masterIP = job.addService(TachyonMasterService())
    job.addChildJobFn(tachyon_worker, numWorkers, masterIP)


def tachyon_workers(job, numWorkers, masterIP):

    for i in range(len(numWorkers)):
        job.addService(TachyonWorkerService(masterIP))

    job.addChildJobFn(hello_world)

def hello_world(job):

    print "Hello World"


# SERVICE CLASSES

class SparkMasterService(Job.Service):

    def start(self):
        call(["docker", "build", "apache-spark-master"])
        startString = check_output(["docker", "run", "apache-spark-master"])
        return self.startString
        
    def stop(self):
        # SOME DOCKER END COMMAND
        return self.stopString

    
class SparkWorkerService(Job.Service):
    def __init__(masterIP):
        Job.Service.__init__(self)
        self.masterIP = masterIP

    def start(self):
        call(["docker", "build", "apache-spark-worker"])
        startString = check_output(["docker", "run", "apache-spark-worker", masterIP])
        return self.startString
        
    def stop(self):
        # SOME DOCKER END COMMAND
        return self.stopString

class TachyonMasterService(Job.Service):

    def start(self):
        call(["docker", "build", "tachyon-master"])
        self.startString = check_output(["docker", "run", "tachyon-master"])
        return self.startString

    def stop(self):
        # SOME DOCKER END COMMAND
        return self.stopString
    
class TachyonWorkerService(Job.Service)
    def __init__(masterIP):
        Job.Service.__init__(self)
        self.masterIP = masterIP

    def start(self):
        call(["docker", "build", "tachyon-worker"])
        self.startString = check-output(["docker", "run", "tachyon-worker", masterIP])
        return self.startString
    
    def stop(self):
        # SOME DOCKER END COMMAND
        retrun self.stopString
        

def main(args):

    Job.Runner.startToil(Job.wrapJobFn(initialize))

if __name__=="__main__":
    sys.exit(main(sys.argv[1:]))
