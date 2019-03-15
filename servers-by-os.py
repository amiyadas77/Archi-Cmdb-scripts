#Script to pull out all server nodes with their OS, number of CPUs, memory size and vCluster
#Also row for each software installed

import sys
import uuid
import csv
from cmdbconstants import *

depends = dict() #Keyed by dependant (parent), set of (dependency (child), relationship)
allRels = set() #Set of all relationships (parent, rel, child) 
appRelSet = set() #Set of dependencies (parent, relationship, child) for an application
classByNode = dict() # Keyed by node id, the Cmdb class of the node
serverList = set() # set of server ids
vCluster = set() # set of TechnologyCollaboration that are a Vmware cluster
serverVcluster = dict() # Keyed by serverId, the vClusterId it belongs to
sysSoftware = set() # set of system software ids
serverSoftware = set() # set of (serverId, softwareId)
appsSoftwareWithNodes = dict() # Keyed by "(apps id, child id)" the set of nodes with that combination (i.e. node is child of an app and hosts the software) 
nodeDescById = dict() #dict of node descriptions keyed by id

#Read in Archie nodes from exported file
fnodes = open("elements.csv", "r")
count = 0
prevStr = False
for lstr in fnodes:
	count += 1
	if count == 1: continue
	if lstr.count('"') % 2 == 1:
		#Multi-line entry
		fullStr += lstr
		if not prevStr: #First line (else last)
			prevStr = True
			continue
	elif prevStr:
		#Continuing line 
		fullStr += lstr
		continue
	else : #Not multi-line
		fullStr = lstr
	prevStr = False
	#print fullStr
	csvList = list()
	csvList.append(fullStr)
	#fs = csv.reader(csvList, delimiter=',', quotechar = '"').next()
	fs = fullStr.rstrip('\n\r').split(",")
	fullStr = ''
	#print fs[0], fs[1], fs[2], fs[3]
	name = fs[2].strip('"')
	lowerName = name.lower()
	nodeType = fs[1].strip('"')
	id = fs[0].strip('"')
	nodesById[id] = (name, nodeType)
	desc = ''
	for n in range(3, len(fs)): #Combine remaining fields - description field with commas
		if n != 3: desc += ',' #Add back in comma removed by split
		desc += fs[n]
	nodeDescById[id] = desc
	if nodeType == "SystemSoftware":
		sysSoftware.add(id)
	if nodeType == "TechnologyCollaboration" and "vcluster" in lowerName:
		vCluster.add(id)
	
fnodes.close

#Read in existing properties
fprops = open("properties.csv")
count = 0
lstr = ""
for line in fprops:
	count += 1
	if count == 1: continue
	lstr += line.rstrip('\n\r')
	if lstr.endswith('"'):
		fs = lstr.split(",")
		id = fs[0].strip('"')
		name = fs[1].strip('"')
		val = fs[2].strip('"')
		propKey = (id, name)
		props[propKey] = val
		#Pull out servers
		if name == classPropName:
			classByNode[id] = val
			if val in serverClasses : serverList.add(id)
	lstr = ""
fprops.close

#Read in relationships and create dependency subtrees
#These relationships are used in the next pass of the relations file to add in sub-relations if 
#the relationship is a pass-thru relationship. This is when the CMDB data model cannot handle a particular
#relationship or class type but sub-relationships might.
frels = open("relations.csv")
count = 0
lstr = ""
for line in frels:
	count += 1
	if count == 1: continue
	lstr += line.rstrip('\n\r')
	if lstr.endswith('"'):
		fs = lstr.split(",")
		#relKey = fs[4].strip('"') + "-" + fs[1].strip('"') + "-" + fs[5].strip('"')
		relId = fs[0].strip('"')
		srcId = fs[4].strip('"')
		type = fs[1].strip('"')
		targetId = fs[5].strip('"')
		if type == "ServingRelationship" or type == "RealizationRelationship":
			rel = (srcId, targetId, type)
			if targetId in depends: 
				depends[targetId].add(rel)
			else: 
				depends[targetId] = set([rel])
		elif type == "AssignmentRelationship" or type == "AggregationRelationship" or type == "CompositionRelationship":
			if srcId in vCluster and targetId in serverList:
				serverVcluster[targetId] = srcId
			rel = (targetId, srcId, type)
			if srcId in depends: 
				depends[srcId].add(rel)
			else: 
				depends[srcId] = set([rel])
		allRels.add(rel)
		#print rel
		lstr = ""
frels.close

def extractCPUMem(nodeId):
	#print nodeName, docStr
	desc = str(nodeDescById[nodeId].strip('"'))
	#docStr = unicode(docStr,'ascii', 'ignore')
	#desc = unicode(desc, 'utf-8', errors='ignore')
	#desc = desc.encode('ascii', 'ignore')
	desc = desc.replace('\r', '\n')
	lines = desc.split("\n")
	cpu = ''
	mem = ''
	for line in lines:
		if "cpu" in line.lower():
			words = line.split()
			for word in words:
				if word.isdigit():
					cpu = word
				if "cpu" in word.lower():
					break
			#RAM spec must be in the same line as CPU
			if "gb" in line.lower() or "ram" in line.lower():
				words = line.split()
				for word in words:
					if word.isdigit():
						mem = word
					if "gb" in word.lower() or "ram" in word.lower():
						break
		if cpu != '':
			break
	return (cpu, mem)

#For each server :
#Find all software associated with the server
#Create a tuple of (server, software)

for serverId in serverList:
	rels = depends.get(serverId, set())
	for rel in rels:
		childId = rel[0]
		if childId in sysSoftware:
			serverSoftware.add((serverId, childId))

fss = open("server-software.csv", "w")
print >>fss, "Server, Status, Type, Software, CPU, Memory, vCluster"
for (serverId, softwareId) in serverSoftware:
	type = revClassLookup.get(props.get((serverId, classPropName),'None'), 'None')
	(cpu, mem) = extractCPUMem(serverId)
	status = props.get((serverId, opStatusName), '')
	vClusterId = serverVcluster.get(serverId, 'None')
	vCluster = nodesById.get(vClusterId, ('None', ''))[0]
	print >>fss, '%s,%s,%s,%s,%s,%s,%s' % (nodesById[serverId][0], status, type, nodesById[softwareId][0],cpu, mem, vCluster)
fss.close

