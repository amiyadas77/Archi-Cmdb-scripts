#Archie EA tool to Snow CMDB synchronisation script
#Creates CMDB relationship import file based on all dependencies for all business services and applications
#Note: Snow CMDB does not support transitive dependencies and so everything must be explicitly defined.
#Author: Danny Andersen

#TODO: Compare CMDB relationship export with relationships and only add entries that are different / new / dropped

import sys
import uuid

rels = dict()
props = dict() #Keyed by (node id, property name)
nodesByName = dict() #Keyed by node name, id of node
nodesById = dict() # Keyed by node id, name of node
cmdb = dict() #Keyed by Name, cmdb id
archieIdtoCmdbId = dict() #keyed by Archie id, cmdb id
alwaysDepends = dict() #"Always" connection, Keyed by dependant, set of (dependency, outage, relationship)
clusterDepends = dict() #"Cluster" connection, Keyed by dependant, set of (dependany, outage, relationship)
occasionalDepends = dict() #"Occasional" connection, Keyed by dependant, set of (dependany, outage, relationship)
infreqDepends = dict() #"Infrequent" connection, Keyed by dependant, set of dependancies
cmdbRelSet = set() #Set of dependencies (parent, relationship, child, strength, outage)
missingFromCmdb = set() # Set of node names missing from CMDB

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

classPropStr = "CMDB Class"
osName = "CMDB Operating System"
strengthPropStr = "CMDB-REL Strength"
outagePropStr = "CMDB-REL Outage"
deviceTypeName = "CMDB Device Type"
osName = "CMDB Operating System"
fnName = "CMDB Function"
ipName = "CMDB IP Address"
statusName = "CMDB Status"

alwaysStr = "Always"
clusterStr = "Cluster"
occStr = "Occasional"
infreqStr = "Infrequent"

computerClass = "Computer"
printerClass = "Printer"
appStr = "cmdb_ci_appl"
appClass = "Application"
businessStr = "cmdb_ci_service"
busServiceClass = "Business Service"
busOfferStr = "service_offering"
busOfferingClass = "Service Offering"
serverStr = "cmdb_ci_server"
serverClass = "Server"
esxServerStr = "cmdb_ci_esx_server"
esxServerClass = "ESX Server"
aixServerStr = "cmdb_ci_aix_server"
aixServerClass = "AIX Server"
dbStr = "cmdb_ci_database"
dbClass = "Database"
dbInstStr = "cmdb_ci_db_instance"
dbInstClass = "Database Instance"
dbOraStr = "cmdb_ci_db_ora_instance"
dbOraClass = "Oracle Instance"
dbSqlStr = "cmdb_ci_db_mssql_instance"
dbSQLClass = "MSFT SQL Instance"
db2DbStr = "cmdb_ci_db_db2_instance"
mySqlDbStr = "cmdb_ci_db_mysql_instance"
sybDbStr = "cmdb_ci_db_syb_instance"
linuxClass = "Linux Server"
linuxStr = "cmdb_ci_linux_server"
solarisClass = "Solaris Server"
solarisStr = "cmdb_ci_solaris_server"
netClass = "Network Gear"
netStr = "cmdb_ci_netgear"
winClass = "Windows Server"
winStr = "cmdb_ci_win_server"
storageDevClass = "Storage Device"
storageDevStr = "cmdb_ci_storage_device"
storageServerClass = "Storage Server"
storageServerStr = "cmdb_ci_storage_server"
sanSwitchStr = "cmdb_ci_storage_switch"
sanSwitchClass = "Storage Switch"
sanFabricClass = "SAN Fabric"
sanFabricStr = "cmdb_ci_storage_switch"
sanClass = "Storage Area Network"
sanStr = "cmdb_ci_san"
containerClass = "Storage Container Object"
containerStr = "cmdb_ci_container_object"
netgearStr = "cmdb_ci_netgear"
subnetStr = "cmdb_ci_subnet"
lbhwStr = "cmdb_ci_lb"
lbhwClass = "Load Balancer"
lbswStr = "cmdb_ci_lb_appl"
lbswClass = "Load Balancer Application"
groupStr = "cmdb_ci_group"
vcenterClass = "VMware vCenter Instance"
vcenterStr = "cmdb_ci_vcenter"
vmwareClass = "VMware Virtual Machine Instance"
vmwareStr = "cmdb_ci_vmware_instance"
vmClass = "Virtual Machine Instance"
vmStr = "cmdb_ci_vm_instance"
lparServerStr = "cmdb_ci_mainframe_lpar"
clusterStr = "cmdb_ci_cluster"
clusterClass = "Cluster"
rackStr = "cmdb_ci_rack"
rackClass = "Rack"

servingStr = "ServingRelationship"
compositionStr = "CompositionRelationship"
specialStr = "SpecialisationRelationship"
company = "NIE Networks"

dependsOnStr = "Depends on::Used By"
runsOnStr = "Runs on::Runs"
hostedOnStr = "Hosted on::Hosts"
clusterOfStr = "Cluster of::Cluster"
ipConnectionStr = "IP Connection::IP Connection"
storageStr = "Provides storage for::Stored on"

#Add dependencies for passed in id, recursing down the tree - each dependency keyed by dependant, set of (dependency, outage, relationship)
def addDepend(id, subId):
	#print "app id: %s - dependent id %s\n" % (id, subId)
	if subId in alwaysDepends:
		for dependency in alwaysDepends[subId]:
			#print "A,%s,%s,%s" % (nodesById[id], nodesById[subId], nodesById[dependency[0]])
			cmdbRelSet.add((id, dependency[2], dependency[0], alwaysStr, dependency[1]))
			addDepend(id, dependency[0])
	if subId in clusterDepends:
		for dependency in clusterDepends[subId]:
			#print "C,%s,%s,%s" % (nodesById[id], nodesById[subId], nodesById[dependency[0]])
			cmdbRelSet.add((id, dependency[2], dependency[0], clusterStr, dependency[1]))
			addDepend(id, dependency[0])
	if subId in occasionalDepends:
		for dependency in occasionalDepends[subId]:
			#print "O,%s,%s,%s" % (nodesById[id], nodesById[subId], nodesById[dependency[0]])
			cmdbRelSet.add((id, dependency[2], dependency[0], occStr, dependency[1]))
			addDepend(id, dependency[0])
	if subId in infreqDepends:
		for dependency in infreqDepends[subId]:
			#print "I,%s,%s,%s" % (nodesById[id], nodesById[subId], nodesById[dependency[0]])
			cmdbRelSet.add((id, dependency[2], dependency[0], infreqStr, dependency[1]))
			addDepend(id, dependency[0])

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
#print "Existing nodes: " + str(count) + ": " + str(len(nodes))

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
		if name == classPropStr:
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
for line in fcmdb:
	count += 1
	if count == 1: continue
	fields=line.rstrip("\n\r").split(",")
	cmdbId = fields[0]
	name = fields[1].lower()
	status = fields[5]
	if status == "Retired": continue
	if name in nodesByName:
		nodeId = nodesByName[name]
		archieIdtoCmdbId[nodeId] = cmdbId
fcmdb.close

#Read in relationships and create dependency subtrees based on "CMDB-REL Strength" prop
#ServingRelationship -> store source against target 
#CompositionRelationship -> store target against source if src is Application (app depends on sub apps), 
#	or store source against target if source is server (VM depends on physical)
#SpecialisationRelationship -> store target against source
#AggregationRelationship -> ignore
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
		relStrenPropKey = (relId, strengthPropStr)
		depend = alwaysDepends
		outage = 100
		if relStrenPropKey in props : 
			if props[relStrenPropKey] == clusterStr:
				depend = clusterDepends
				relOutagePropKey = (relId, outagePropStr)
				if relOutagePropKey in props:
					outage = props[relOutagePropKey]
			elif props[relStrenPropKey] == occStr:
				depend = occasionalDepends
			elif props[relStrenPropKey] == infreqStr:
				depend = infreqDepends
		if type == servingStr: 							#Serving relationship
			relType = dependsOnStr
			if srcId in serverList and (targetId in appsList or targetId in dbList):
				relType = runsOnStr #App runs on server
			if targetId in depend:
				depend[targetId].add((srcId, outage, relType))
			else:
				depend[targetId] = set([(srcId, outage, relType)])
		elif type == compositionStr:  					#Composition
			if srcId in appsList or srcId in busServicesList or srcId in groupList:
				#Application or Bus service composed of others, add target to source
				if srcId in depend:
					depend[srcId].add((targetId, outage, dependsOnStr))
				else:
					depend[srcId] = set([(targetId, outage, dependsOnStr)])
			elif srcId in serverList:
				#Server is part of another server, e.g. lpar in host, add source to target
				if targetId in depend: 
					depend[targetId].add((srcId, outage, dependsOnStr))
				else:
					depend[targetId] = set([(srcId, outage, dependsOnStr)])
		elif type == specialStr: 						#Specialisation
			if targetId in depend:
				depend[targetId].add((srcId, outage, dependsOnStr))
			else:
				depend[targetId] = set([(srcId, outage, dependsOnStr)])
		lstr = ""
frels.close

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
		missingFromCmdb.add((nodesById[parent], (parent, classPropStr) in props))
		continue
	child = d[2]
	if child not in archieIdtoCmdbId:
		missingFromCmdb.add((nodesById[child],(child, classPropStr) in props))
		continue
	propKey = (child, classPropStr)
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
