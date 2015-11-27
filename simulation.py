import collections
import random
import simpy
import multiprocessing as mp
import os
import datetime



def create(env, requests_info):
	#Loop to create the compute nodes
	global object_servers, compute_nodes, file_name
	filenames_size=[{"A":100},{"B":100},{"C":100},{"D":100}]
	for i in range(PROCESSORS):
		compute_nodes.append(ComputeNodes(env,i+1))
	for i in range(OBJECT_SERVERS):
		object_servers.append(ObjectServers(env,i+1, 10, 10))
	for i in range(len(object_servers)):
		object_servers[i].updateFileTable(filenames_size[i])
	try:
		yield env.process(head(env, requests_info))
	except ValueError:
			print("DONE")	


def head(env,requests_info):
	global compute_nodes
	for request in requests_info:
		for processor in requests_info[request]:
			compute_nodes[processor-1].setMembers(requests_info[request][processor])
		# DO 1st REQUEST computations
		for processor in requests_info[request]:
			compute_nodes[processor-1].display()
		
		#yield env.process(distribute())
		try:
			yield env.process(parallel_proc(env))
		except ValueError:
			print("DONE")



def parallel_proc(env):
	global read_times, write_times, computation_times
	for proc in range(PROCESSORS):
		parallelread(compute_nodes[proc])
	for objno in range(len(object_servers)):
		read_times.append(object_servers[objno].getTotalReadTime())
	yield env.timeout(max(read_times))
	
	for proc in range(PROCESSORS):
		parallelcomputation(compute_nodes[proc])
	yield env.timeout(max(computation_times))
	for proc in range(PROCESSORS):
		parallelwrite(compute_nodes[proc])
	for objno in range(len(object_servers)):
		write_times.append(object_servers[objno].getTotalWriteTime())
	yield env.timeout(max(write_times))	
	for objno in range(len(object_servers)):
		object_servers[objno].finalLogging()	
	for objno in range(len(object_servers)):
		object_servers[objno].getLog()	
	for objno in range(len(object_servers)):
		object_servers[objno].thresholdCalc()
		#print("THRESHOLD :",object_servers[objno].thresholdCalc(),"LAT of ",objno,object_servers[objno].LastAccessTime)
		object_servers[objno].LastAccessTime = env.now
	print("ENV NOW :",env.now)

	print("System now is intelligent....")

##############################################################################

	for proc in range(PROCESSORS):
		parallelread(compute_nodes[proc])
	for objno in range(len(object_servers)):
		read_times.append(object_servers[objno].getTotalReadTime())
	yield env.timeout(max(read_times))
	#print("ENV NOW :",env.now)
	for proc in range(PROCESSORS):
		parallelcomputation(compute_nodes[proc])
	yield env.timeout(max(computation_times))
	for proc in range(PROCESSORS):
		parallelwrite(compute_nodes[proc])
	for objno in range(len(object_servers)):
		write_times.append(object_servers[objno].getTotalWriteTime())
	yield env.timeout(max(write_times))		
	for objno in range(len(object_servers)):
		object_servers[objno].finalLogging()	
	for objno in range(len(object_servers)):
		object_servers[objno].getLog()		

	
		
def parallelread(node):
	global read_times
	for file_ in node.actions:
		if(node.actions[file_] == "read"):
			obj_server = object_servers[(int(node.locations[file_]))-1]
			obj_server.read(file_)

def parallelcomputation(node):
	computation_times.append(node.computation())
	
def parallelwrite(node):
	global write_times
	for file_ in node.actions:
		if(node.actions[file_] == "write"):
			obj_server = object_servers[(int(node.locations[file_]))-1]
			obj_server.write(file_)
	
					

def location(file_lookup):#file_lookup-filename to be searched
	#lookup{diskname:[filename,filename]}
	fp1=open("Table1","r")
	lines=fp1.read()
	lines=lines.split("\n")
	lines.remove('')
	for line in lines:
		content=line.split(":")
		lookup[content[0]]=content[1]

	fp1.close() 
	for key in lookup.keys():
		if lookup[key]==file_lookup:
			print("found file at disk",key)
	return key

def threshold():
	fp2.write("Filename	Diskname	Action	Time\n")
	#call object server method to fill the action and time fields
	fp2.write("------------------------------------------\n")
	for i in file_name:	
		d=objserver(i)
		d_name=location(d["file_name"])
		fp2.write(d["file_name"]+"\t"+d_name+"\t"+"Read"+"\t"+str(d["time"])+"\n")

def objserver(f_name):
	while True:
		#print ("start reading at : %d"%now)
		global now
		now+=2
		return {"file_name":f_name,"time":now}


def metadata():
	global lookup
	loc={}
	for i in lookup:
		loc[lookup[i]]=i
	print(loc)
	return loc
							
	

class ObjectServers(object):
	def __init__(self,env,slno, readTime, writeTime):
		self.env = env
		self.slno = slno
		self.log=[] #Colon separated values. FileName:Action:Time
		self.fileTable={}
		self.thresholdValue=100000000000
		self.readTime = readTime
		self.writeTime = writeTime
		self.LastAccessTime = 0	
		self.state = "DUMB"
		self.totalReadTime=0
		self.totalWriteTime=0

	def updateFileTable(self,table):
		self.fileTable=table	

	def read(self, infile):
		#print("Read is being called in " + str(self.slno))	
		Rtime=0
		Itime = env.now - self.LastAccessTime
		if(self.state == "INTELLIGENT"):
			
			if(Itime <= self.thresholdValue):
				if(Itime > 0):
					self.logging("DiskK","I",Itime)
			else:
				self.logging("Disk","I",self.thresholdValue)
				self.logging("Disk","SD",(Itime - self.thresholdValue))
		else:
			if(Itime>0):	
				self.logging("Disk","I",Itime)
		Rtime = int(self.fileTable[infile])/self.readTime		
		self.logging(infile,"R",Rtime)
		self.totalReadTime=self.totalReadTime+Rtime
		self.LastAccessTime = env.now + self.totalReadTime 
		#self.totalReadTime=self.totalReadTime+Rtime
	
	def write(self, infile):
		#print("Write is being called")
		Wtime=0
		Itime = env.now - self.LastAccessTime
		if(self.state == "INTELLIGENT"):
			
			if(Itime <= self.thresholdValue):
				if(Itime > 0):
					self.logging("Disk","I",Itime)
			else:
				self.logging("Disk","I",self.thresholdValue)
				self.logging("Disk","SD",(Itime - self.thresholdValue))
		else:
			if(Itime>0):	
				self.logging("Disk","I",Itime)
		Wtime = int(self.fileTable[infile])/self.writeTime		
		self.logging(infile,"W",Wtime)
		self.totalWriteTime=self.totalWriteTime+Wtime		
		self.LastAccessTime = env.now + self.totalWriteTime

	def finalLogging(self):
		if(self.LastAccessTime < env.now):
			Itime = env.now - self.LastAccessTime
			if(Itime <= self.thresholdValue):
				if(Itime > 0):
					self.logging("Disk","I",Itime)
			else:
				self.logging("Disk","I",self.thresholdValue)
				self.logging("Disk","SD",(Itime - self.thresholdValue))
		
		
		

	def logging(self,filename,action,time):
		entry = filename+":"+action+":"+str(time)
		self.log.append(entry)

	def getLog(self):
		print("PROCESSOR NO "+str(self.slno)+" :"+str(self.log))
		
	

	def thresholdCalc(self):
		runningTimeList=[]
		for entry in self.log:
			entry_elements = entry.split(":")
			if( entry_elements[1] == "I" ):
				runningTimeList.append(int(entry_elements[2]))
		self.thresholdValue = sum(runningTimeList)/4
		self.state = "INTELLIGENT"
		self.log=[]
		if(self.thresholdValue == 0):
			self.thresholdValue = 1000000
		return self.thresholdValue
		
	def getTotalReadTime(self):
		trt = self.totalReadTime
		self.totalReadTime = 0
		#self.LastAccessTime = env.now + trt
		return trt

	def getTotalWriteTime(self):
		twt = self.totalWriteTime
		self.totalWriteTime = 0
		#self.LastAccessTime = env.now + twt
		return twt

class ComputeNodes(object):
	def __init__(self,env,slno):
		self.env = env
		self.actions = {}
		self.locations = {}
		self.slno = slno

	def setMembers(self,file_actions):
		self.actions = file_actions
		self.locations = metadata()
		## Call meta-data server to get all information		

	def display(self):
		print("PROCESSOR NO : "+str(self.slno)+"  ACTIONS : "+ str(self.actions));

	def computation(self):
		#retrieve the computation time from metadata server or randomly generate some computation time each time
		#advance the clock by the computation time
		#print("computing") 
		return 200
		
if __name__ == '__main__' :
	REQUESTS=1
	PROCESSORS=4
	REQUESTS_INFO={"req1":{1:{"A":"read","B":"write"},2:{"B":"write"},3:{"C":"read","A":"read"},4:{"A":"write","D":"read"}}}
	read_times=[]
	write_times=[]
	computation_times=[]
	compute_nodes=[]
	object_servers=[]
	OBJECT_SERVERS=4
	d={}
	fp2=open("Table2","w")
	file_name=["A","B","C","D"]
	now=0
	lookup={}
	content=[]
	
	env = simpy.Environment()
	threshold() #### see if this is alright
	a=create(env,REQUESTS_INFO)
	#env.process(a)
	try:
		env.process(a)
	except ValueError:
		print("heloooooooooo")
		
	env.run()
