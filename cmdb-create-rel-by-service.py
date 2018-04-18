#Archie EA tool to Snow CMDB synchronisation script
#Creates CMDB relationship import file based on all dependencies for all business services and applications
#Note: Snow CMDB does not support transitive dependencies and so everything must be explicitly defined.
#Author: Danny Andersen

#TODO: Compare CMDB relationship export with relationships and only add entries that are different / new / dropped

import sys
import uuid
import csv
from cmdbconstants import *

allowedRels = dict() # Keyed by (classFrom, classTo, ArchieRelationship) value = (CMDB Rel, parent->child = True)
archieIdtoCmdbId = dict() #keyed by Archie id, cmdb id
# alwaysDepends = dict() #"Always" connection, Keyed by dependant, set of (dependency, outage, relationship)
# clusterDepends = dict() #"Cluster" connection, Keyed by dependant, set of (dependany, outage, relationship)
# occasionalDepends = dict() #"Occasional" connection, Keyed by dependant, set of (dependany, outage, relationship)
# infreqDepends = dict() #"Infrequent" connection, Keyed by dependant, set of dependancies
depends = dict() #Keyed by dependant (parent), set of (dependency (child), relationship, strength, outage)
cmdbRelSet = set() #Set of dependencies (parent, relationship, child, strength, outage)
missingFromCmdb = set() # Set of node names missing from CMDB
classByNode = dict() # Keyed by node id, the Cmdb class of the node
cmdbToArchiRels = dict() # Keyed by (classFrom, classTo, CMDB Rel) value = (ArchieRelationship, parent->child = True)

appsList = list() # list of application ids
serverList = list() # list of server ids
busServicesList = list() # list of business services ids
dbServerList = list() #list of db ids
vmList = list() # list of VM ids
vcenterList = list() # list of vCenter ids
sanList = list() #List of storage kit
sanFabricList = list() #List of storage kit
dbList = list() # list of dbs
containerList = list() # list of storage containers
groupList = list() # list of clusters or groups
rackList = list() # list of racks
hwlbList = list() # list of HW loadbalancers
swlbList = list() # list of SW loadbalancers


dependsOnStr = "Depends on::Used By"
runsOnStr = "Runs on::Runs"
hostedOnStr = "Hosted on::Hosts"
clusterOfStr = "Cluster of::Cluster"
ipConnectionStr = "IP Connection::IP Connection"
storageStr = "Provides storage for::Stored on"


#Add dependencies for passed in id, recursing down the tree - each dependency keyed by dependant, set of (dependency, outage, relationship)
def addDepend(id, subId):  #Note: Id = Super parent, subId = add all subIds as children
	global cmdbRelSet
	global depends
	global allowedRels
	#print "%s: Adding children of %s" % (nodesById[id], nodesById[subId])
	rels = depends.get(subId, set())
	for rel in rels:
		type = rel[2]
		child = rel[0]
		srcClass = classByNode.get(child, 'NONE')
		targetClass = classByNode.get(subId, 'NONE')
		relByClass = (srcClass, targetClass, type)
		#print rel, relByClass, relByClass in allowedRels
		#print "%s:%s, %s:%s, %s" % (nodesById[child], srcClass, nodesById[subId], targetClass, nodesById[rel[0]])
		#print "%s,%s,%s" % (nodesById[child], nodesById[subId], rel)
		#cmdbRel -> (parent, relationship, child, strength, outage)
		#rel => (dependency (child), relationship, strength, outage)
		if relByClass in allowedRels or srcClass == 'NONE' or targetClass == 'NONE':
			if type == passThruStr or srcClass == 'NONE' or targetClass == 'NONE':
				print "%s: Pass thru adding children of : %s" % (nodesById[id], nodesById[child])
				addDepend(id, child) #pass thru of a pass thru - add childrens children
			else:
				newRel = (id, type, child, rel[3], rel[4])
				#print "Adding %s,%s,%s" % (nodesById[id], newRel[1], nodesById[child])
				cmdbRelSet.add((newRel[0], newRel[1], newRel[2], newRel[3], newRel[4]))
				addDepend(id, child)

#Read in Archie nodes from exported file
fnodes = open("elements.csv", "r")
count = 0
for line in fnodes:
	count += 1
	if count == 1: continue
	lstr = line.rstrip('\n\r')
	fs = lstr.split(",")
	if len(fs) < 3 or not (fs[0].startswith('"')) : continue
	#print fs[0], fs[1], fs[2]
	name = fs[2].strip('"').lower()
	id = fs[0].strip('"')
	nodesByName[name] = id
	nodesById[id] = name
fnodes.close

#Read in allowed relationships rules
#(classFrom, classTo, ArchieRelationship) value = (CMDB Rel, parent->child = True)
frules = open("rel-rules.csv")
count = 0
for line in frules:
	count += 1
	if count == 1: continue
	fs = line.strip('\n\r').split(",")
	srcRel = fs[0].strip()
	targetRel = fs[1].strip()
	archieRel = fs[2].strip()
	type = fs[3].strip()
	keepDirRel = fs[4].strip().lower() == 'true'
	allowedRels[(srcRel, targetRel, archieRel)] = (type, keepDirRel)
	cmdbToArchiRels[(srcRel, targetRel, type)] = (archieRel, keepDirRel)
frules.close

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
		if name == classPropName:
			classByNode[id] = val
			if val == appStr: appsList.append(id)
			elif val == serverStr or val == esxServerStr or val == aixServerStr \
							or val == linuxStr or val == solarisStr or val == winStr or val == lparServerStr: serverList.append(id)
			elif val == vmwareStr or val == vmStr: vmList.append(id)
			elif val == vcenterStr: vcenterList.append(id)
			elif val == businessStr or val == busOfferStr: busServicesList.append(id)
			elif val == dbInstStr or val == dbSqlStr or val == dbOraStr or val == dbStr \
				or val == db2DbStr or val == mySqlDbStr or val == sybDbStr : dbList.append(id)
			elif val == sanSwitchStr: sanFabricList.append(id)
			elif val == sanFabricStr: sanFabricList.append(id)
			elif val == sanStr: sanList.append(id)
			elif val == storageServerStr: sanList.append(id)
			elif val == storageDevStr: sanList.append(id)
			elif val == containerStr: containerList.append(id)
			elif val == clusterStr or val == groupStr: groupList.append(id)
			elif val == rackStr: rackList.append(id)
			elif val == lbhwStr: hwlbList.append(id)
			elif val == lbswStr: swlbList.append(id)
			elif val == netgearStr or val == subnetStr: pass
			else: print "Not accounted for cmdb class: %s\n" % val

		lstr = ""
fprops.close

#Store all cmdbIds by name
fcmdb = open("SNOW CMDB.csv")
count = 0
fullStr = ""
prevStr = False
cols = dict()
for lstr in fcmdb:
	count += 1
	if count == 1:
		cols = processHeader(lstr)
		continue
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
	csvList = list()
	csvList.append(fullStr)
	fields = csv.reader(csvList, delimiter=',', quotechar = '"').next()
	#fields = fullStr.rstrip('\n\r').split(",")
	#print fields[0], fields[1], fields[2], fields[3]
	fullStr = ''
	cmdbId = fields[cols[propRevLookup[cmdbIdName]]]
	name = fields[cols["Name"]].lower()
	status = fields[cols[propRevLookup[statusName]]]
	if status == "Retired": continue
	if name in nodesByName:
		nodeId = nodesByName[name]
		archieIdtoCmdbId[nodeId] = cmdbId
fcmdb.close

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
		srcClass = classByNode.get(srcId, 'NONE')
		targetClass = classByNode.get(targetId, 'NONE')
		strength = props.get((relId, strengthPropStr), alwaysStr)
		outage = "100"
		if strength == clusterRelStr:
			outage = props.get((relId, outagePropStr), "0")
		if srcClass == 'NONE' or targetClass == 'NONE': 
			relByClass = ('NONE', 'NONE', type)
		else: 
			relByClass = (srcClass, targetClass, type)
		if relByClass in allowedRels: 
			#Relationship is allowed by datamodel or a passthru
			cmdbRel = allowedRels[relByClass]
			if cmdbRel[1]: # Parent->Child = Src->Target
				rel = (targetId, cmdbRel[0], type, strength, outage)
				if srcId in depends: depends[srcId].add(rel)
				else: depends[srcId] = set([rel])
			else: # Parent->Child = Target->Src i.e. swap the direction of relationship
				rel = (srcId, cmdbRel[0], type, strength, outage)
				if targetId in depends: depends[targetId].add(rel)
				else: depends[targetId] = set([rel])
		lstr = ""
frels.close

#print allowedRels
print "Dependencies: %d, Rules: %d" % (len(depends), len(allowedRels))

#For each app / bus service node:
#Find all "serving" rels that have target of node, add them to dependency set
#Find their child "serving" rels and add them to the dependency set 
for busServiceId in busServicesList:
	addDepend(busServiceId, busServiceId)

for appId in appsList:
	addDepend(appId, appId)

frels = open("cmdb-relations-by-service.csv", "w")
freadable = open("readable-relations-by-service.csv", "w")
freltemplate = open("cmdb-relations-template.csv")
for t in freltemplate:
	print >>frels,t
freltemplate.close
print >>freadable, "Parent, Child, Child Class, Child OS, Child OS Family, Child Device, Child Function, Child IP Addr, Relationship, Strength, Outage"
for d in cmdbRelSet:
	#operation,p_unique_id,p_class,p_name,p_company,type,c_unique_id,c_class,c_name,c_company,connection_strength,percent_outage,u_schedule,,,,,,,
	# d= (parent, relationship, child, strength, outage)
	parent = d[0]
	if parent not in archieIdtoCmdbId:
		missingFromCmdb.add((nodesById[parent], (parent, classPropName) in props))
		continue
	child = d[2]
	if child not in archieIdtoCmdbId:
		missingFromCmdb.add((nodesById[child],(child, classPropName) in props))
		continue
	propKey = (child, classPropName)
	childClass = props.get(propKey, '')
	if childClass == lparServerStr: childClass = aixServerStr  #Convert class to AIX rather than the mainframe lpar
	fn = props.get((child, fnName), '')
	if fn == "Unknown": fn = ''
	ipAddress = props.get((child, ipName), '')
	status = props.get((child, statusName), '')
	devType = props.get((child, deviceTypeName), '')
	if devType == "Unknown": devType = ''
	if ipAddress == "Unknown": ipAddress = ''
	childOs = props.get((child, osName), '')
	if childOs == "Unknown": childOs = ''
	osFamily = ''
	oslower = childOs.lower()
	if "windows" in oslower: osFamily = "Windows"
	elif "linux" in oslower: osFamily = "Linux"
	elif "aix" in oslower or "unix" in oslower or "sun" in oslower: osFamily = "Unix"
	elif "esx" in oslower: osFamily = "Proprietary"

	print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s' % (nodesById[parent],nodesById[child],childClass,childOs,osFamily,devType,fn,ipAddress, d[1],d[3],d[4])
	#Use the following for actual export to CMDB
	print >>frels, 'create,%s,,,,%s,%s,,,%s,%s,,' % (archieIdtoCmdbId[parent],d[1],archieIdtoCmdbId[child],d[3],d[4])
frels.close	
freadable.close

fmiss = open("cmdb-missing.csv", "w")
print >> fmiss,"Node name, Has Cmdb class?"
for miss in missingFromCmdb:
	print >> fmiss, "%s,%s" % (miss[0], miss[1])
fmiss.close
