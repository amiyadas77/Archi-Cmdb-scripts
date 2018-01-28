#Archie EA tool to Snow CMDB synchronisation script
#Creates CMDB relationship import file based on all archie relationships that comply with the SNOW CMDB data model
#Author: Danny Andersen

#TODO: Compare CMDB relationship export with relationships and only add entries that are different / new / dropped

import sys
import uuid

allowedRels = dict() # Keyed by (classFrom, classTo, ArchieRelationship) value = (CMDB Rel, parent->child = True)
props = dict() #Keyed by (node id, property name)
nodesByName = dict() #Keyed by node name, id of node
nodesById = dict() # Keyed by node id, name of node
classByNode = dict() # Keyed by node id, the Cmdb class of the node
archieIdtoCmdbId = dict() #keyed by Archie node id, cmdb id
cmdbRelSet = set() #Set of dependencies (parent, relationship, child, strength, outage)
depends = dict() #Keyed by dependant (parent), set of (dependency (child), relationship, strength, outage)

missingFromCmdb = set() # Set of node names missing from CMDB
missingRels = set() # Set of archie class relationships not in data model

classPropStr = "CMDB Class"
cmdbIdStr = "CMDB ID"

osName = "CMDB Operating System"
strengthPropStr = "CMDB-REL Strength"
outagePropStr = "CMDB-REL Outage"
deviceTypeName = "CMDB Device Type"
osName = "CMDB Operating System"
fnName = "CMDB Function"
ipName = "CMDB IP Address"
statusName = "CMDB Status"

alwaysStr = "Always"
clusterRelStr = "Cluster"
occStr = "Occasional"
infreqStr = "Infrequent"

company = "NIE Networks"

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
dbOraClass = "Oracle"
dbSqlStr = "cmdb_ci_db_mssql_instance"
dbSQLClass = "MSSQL"
db2DbStr = "cmdb_ci_db_db2_instance"
mySqlDbStr = "cmdb_ci_db_mysql_instance"
sybDbStr = "cmdb_ci_db_syb_instance"
linuxClass = "Linux Server"
linuxStr = "cmdb_ci_linux_server"
netClass = "Network Gear"
netStr = "cmdb_ci_netgear"
winClass = "Windows Server"
winStr = "cmdb_ci_win_server"
storageClass = "Storage Device"
storageServerStr = "cmdb_ci_storage_server"
sanSwitchStr = "cmdb_ci_storage_switch"
containerStr = "cmdb_ci_container_object"
netgearStr = "cmdb_ci_netgear"
subnetStr = "cmdb_ci_subnet"
lbStr = "cmdb_ci_lb_appl"
groupStr = "cmdb_ci_group"
vmwareStr = "cmdb_ci_vmware_instance"
lparServerStr = "cmdb_ci_mainframe_lpar"
clusterStr = "cmdb_ci_cluster"

servingStr = "ServingRelationship"
compositionStr = "CompositionRelationship"
specialStr = "SpecialisationRelationship"
aggregationStr = "AggregationRelationship"

dependsOnStr = "Depends on::Used By"
runsOnStr = "Runs on::Runs"
hostedOnStr = "Hosted on::Hosts"
clusterOfStr = "Cluster of::Cluster"
ipConnectionStr = "IP Connection::IP Connection"
storageStr = "Provides storage for::Stored on"
virtualisedStr = "Virtualized by::Virtualizes"
containedByStr = "Contained By::Contains"
memberOfStr = "Members::Member of"
passThruStr = "PASS-THRU"

#Define CMDB Data Model allowed relationships
#Business Service
# allowedRels[(busOfferStr, busOfferStr, servingStr)] = (dependsOnStr, False)

# #Application Data Model
# allowedRels[(appStr, busOfferStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(dbInstStr, appStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(dbOraStr, appStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(dbSqlStr, appStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(db2DbStr, appStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(mySqlDbStr, appStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(sybDbStr, appStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(appStr, dbInstStr, compositionStr)] = (dependsOnStr, True)
# allowedRels[(appStr, dbOraStr, compositionStr)] = (dependsOnStr, True)
# allowedRels[(appStr, dbSqlStr, compositionStr)] = (dependsOnStr, True)
# allowedRels[(appStr, db2DbStr, compositionStr)] = (dependsOnStr, True)
# allowedRels[(appStr, mySqlDbStr, compositionStr)] = (dependsOnStr, True)
# allowedRels[(appStr, sybDbStr, compositionStr)] = (dependsOnStr, True)
# allowedRels[(appStr, dbInstStr, servingStr)] = (dependsOnStr, True)
# allowedRels[(appStr, dbOraStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(appStr, dbSqlStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(appStr, db2DbStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(appStr, mySqlDbStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(appStr, sybDbStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(serverStr, appStr, servingStr)] = (runsOnStr, False)
# allowedRels[(aixServerStr, appStr, servingStr)] = (runsOnStr, False)
# allowedRels[(linuxStr, appStr, servingStr)] = (runsOnStr, False)
# allowedRels[(winStr, appStr, servingStr)] = (runsOnStr, False)
# allowedRels[(vmwareStr, appStr, servingStr)] = (runsOnStr, False)
# #Physical Server Data Model
# allowedRels[(serverStr, busOfferStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(aixServerStr, busOfferStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(linuxStr, busOfferStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(winStr, busOfferStr, servingStr)] = (dependsOnStr, False)
# #Virtual Server Data Model
# allowedRels[(vmwareStr, busOfferStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(esxServerStr, vmwareStr, compositionStr)] = (virtualisedStr, False)
# allowedRels[(aixServerStr, aixServerStr, compositionStr)] = (virtualisedStr, False)
# #Database Data Model
# allowedRels[(vmwareStr, dbInstStr, servingStr)] = (runsOnStr, False)
# allowedRels[(vmwareStr, dbOraStr, servingStr)] = (runsOnStr, False)
# allowedRels[(vmwareStr, dbSqlStr, servingStr)] = (runsOnStr, False)
# allowedRels[(vmwareStr, db2DbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(vmwareStr, mySqlDbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(vmwareStr, sybDbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(aixServerStr, dbInstStr, servingStr)] = (runsOnStr, False)
# allowedRels[(aixServerStr, dbOraStr, servingStr)] = (runsOnStr, False)
# allowedRels[(aixServerStr, db2DbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(linuxStr, dbInstStr, servingStr)] = (runsOnStr, False)
# allowedRels[(linuxStr, dbOraStr, servingStr)] = (runsOnStr, False)
# allowedRels[(linuxStr, dbSqlStr, servingStr)] = (runsOnStr, False)
# allowedRels[(linuxStr, db2DbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(linuxStr, mySqlDbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(linuxStr, sybDbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(winStr, dbInstStr, servingStr)] = (runsOnStr, False)
# allowedRels[(winStr, dbOraStr, servingStr)] = (runsOnStr, False)
# allowedRels[(winStr, dbSqlStr, servingStr)] = (runsOnStr, False)
# allowedRels[(winStr, db2DbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(winStr, mySqlDbStr, servingStr)] = (runsOnStr, False)
# allowedRels[(winStr, sybDbStr, servingStr)] = (runsOnStr, False)
# #Cluster Data Model
# allowedRels[(winStr, clusterStr, servingStr)] = (memberOfStr, False)
# allowedRels[(vmwareStr, clusterStr, servingStr)] = (memberOfStr, False)
# allowedRels[(aixServerStr, clusterStr, servingStr)] = (memberOfStr, False)
# allowedRels[(linuxStr, clusterStr, servingStr)] = (memberOfStr, False)
# allowedRels[(clusterStr, dbInstStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(clusterStr, appStr, servingStr)] = (dependsOnStr, False)

# #Physical Storage Model
# allowedRels[(storageServerStr, storageServerStr, servingStr)] = (containedByStr, False)
# allowedRels[(storageServerStr, containerStr, servingStr)] = (hostedOnStr, False)
# allowedRels[(containerStr, vmwareStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(containerStr, winStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(containerStr, aixServerStr, servingStr)] = (dependsOnStr, False)
# allowedRels[(containerStr, linuxStr, servingStr)] = (dependsOnStr, False)

# #Pass thru relationships - these are not in the CMDB 
# #and so the depends-on is transitive (as though the child doesn't exist)
# allowedRels[(appStr, appStr, compositionStr)] = (passThruStr, True)
# #allowedRels[('', appStr, servingStr)] = (passThruStr, False)
# #allowedRels[('', busOfferStr, servingStr)] = (passThruStr, False)

#Add dependencies for passed in id, recursing down the tree - each dependency keyed by dependant, set of (dependency, outage, relationship)
def addDepend(id, subId):  #Note: Id = Super parent, subId = add all subIds as children
	#print "app id: %s - dependent id %s\n" % (id, subId)
	rels = depends.get(subId, set())
	for rel in rels:
		#print "%s,%s,%s" % (nodesById[id], nodesById[subId], nodesById[rel[0]])
		#print "%s,%s,%s" % (nodesById[id], nodesById[subId], rel)
		#cmdbRel -> (parent, relationship, child, strength, outage)
		#rel => (dependency (child), relationship, strength, outage)
		type = rel[1]
		child = rel[0]
		if type == passThruStr:
			print "%s: Pass thru - adding children of : %s" % (nodesById[id], nodesById[child])
			addDepend(id, child) #pass thru of a pass thru - add childrens children
		else:
			cmdbRelSet.add((id, type, child, rel[2], rel[3]))

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
	cmdbRel = fs[3].strip()
	keepDirRel = fs[4].strip().lower() == 'true'
	allowedRels[(srcRel, targetRel, archieRel)] = (cmdbRel, keepDirRel)
frules.close

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
		if name == classPropStr: classByNode[id] = val
		if name == cmdbIdStr: archieIdtoCmdbId[id] = val
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
		srcClass = classByNode.get(srcId, '')
		targetClass = classByNode.get(targetId, '')
		strength = props.get((relId, strengthPropStr), alwaysStr)
		outage = 100
		if strength == clusterRelStr:
			outage = props.get((relId, outagePropStr), 0)
		relByClass = (srcClass, targetClass, type)
		if relByClass in allowedRels: 
			#Relationship is allowed by datamodel
			cmdbRel = allowedRels[relByClass]
			if cmdbRel[1]: # Parent->Child = Src->Target
				rel = (targetId, cmdbRel[0], strength, outage)
				if srcId in depends: depends[srcId].add(rel)
				else: depends[srcId] = set([rel])
			else: # Parent->Child = Target->Src i.e. swap the direction of relationship
				rel = (srcId, cmdbRel[0], strength, outage)
				if targetId in depends: depends[targetId].add(rel)
				else: depends[targetId] = set([rel])
		lstr = ""
frels.close

#Read in relationships and create relationships if they are allowed
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
		srcClass = classByNode.get(srcId, '')
		targetClass = classByNode.get(targetId, '')
		outage = 100
		strength = props.get((relId, strengthPropStr), alwaysStr)
		if strength == clusterRelStr:
			outage = props.get((relId, outagePropStr), 0)
		relByClass = (srcClass, targetClass, type)
		if relByClass in allowedRels: 
			#Relationship is allowed by datamodel
			cmdbRel = allowedRels[relByClass]
			passThru = cmdbRel[0] == passThruStr
			#Add relationship (parent, relationship, child, strength, outage)
			if cmdbRel[1]: # Parent->Child = Src->Target
				if passThru: 
					#Need to add in childs relationships directly to the parent
					addDepend(srcId, targetId)
				else:
					cmdbRelSet.add((srcId, cmdbRel[0], targetId, strength, outage))
			else: # Parent->Child = Target->Src
				if passThru: 
					#Need to add in childs relationships directly to the parent
					addDepend(targetId, srcId)
				else:
					cmdbRelSet.add((targetId, cmdbRel[0], srcId, strength, outage))
		else:
			missingRels.add((relByClass, nodesById[srcId], nodesById[targetId]))
			#print "Rel not in Data model: (%s, %s, %s)" % relByClass

		lstr = ""
frels.close

frels = open("cmdb-all-relations.csv", "w")
freadable = open("all-readable-relations.csv", "w")
freltemplate = open("cmdb-relations-template.csv")
for t in freltemplate:
	print >>frels,t
freltemplate.close
#print >>freadable, "Parent, Parent Class, Child, Child Class, Child OS, Child OS Family, Child Device, Child Function, Child IP Addr, Relationship, Strength, Outage"
print >>freadable, "Parent, Parent Class, Child, Child Class, Relationship, Strength, Outage, Parent Missing CMDB CI, Child Missing CMDB CI"
for d in cmdbRelSet:
	#operation,p_unique_id,p_class,p_name,p_company,type,c_unique_id,c_class,c_name,c_company,connection_strength,percent_outage,u_schedule,,,,,,,
	# d= (parent, relationship, child, strength, outage)
	parent = d[0]
	child = d[2]
	propKey = (child, classPropStr)
	childClass = props.get(propKey, '')
	#if childClass == lparServerStr: childClass = aixServerStr  #Convert class to AIX rather than the mainframe lpar
	propKey = (parent, classPropStr)
	parentClass = props.get(propKey, '')
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
	parentMissing = False
	childMissing = False
	if parent not in archieIdtoCmdbId:
		missingFromCmdb.add((nodesById[parent], (parent, classPropStr) in props))
		parentMissing = True
	if child not in archieIdtoCmdbId:
		missingFromCmdb.add((nodesById[child],(child, classPropStr) in props))
		childMissing = True
	#print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (nodesById[parent],parentClass, nodesById[child],childClass,childOs,osFamily,devType,fn,ipAddress, d[1],d[3],d[4])
	print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s' % (nodesById[parent],parentClass, nodesById[child],childClass, d[1],d[3],d[4], parentMissing, childMissing)
	#Use the following for actual export to CMDB
	if not childMissing and not parentMissing:
		print >>frels, 'create,%s,,,,%s,%s,,,%s,%s,' % (archieIdtoCmdbId[parent],d[1],archieIdtoCmdbId[child],d[3],d[4])
frels.close	
freadable.close

fmiss = open("cmdb-missing.csv", "w")
print >> fmiss,"Node name, Has Cmdb class?"
for miss in missingFromCmdb:
	print >> fmiss, "%s,%s" % (miss[0], miss[1])
fmiss.close

fmiss = open("cmdb-missing-rels.csv", "w")
print >> fmiss,"From Class, To Class, Relationship, From Node, To Node"
for miss in missingRels:
	print >> fmiss, "%s,%s,%s,%s,%s" % (miss[0][0], miss[0][1], miss[0][2], miss[1], miss[2])
fmiss.close
