#Snow CMDB synchronisation with Archie - Archie import
#Compares CMDB export with Archie export and creates both Archie and CMDB import entries
#Author: Danny Andersen

import sys
import uuid
import csv
import os
from cmdbconstants import *

propsChanged = dict() #Keyed by id, set of all properties that have changed
allPropsById = dict() #dict of dict of all properties found for a particular element keyed by its id  and the prop name
rels=list() #List of tuples (parent, type, child, name)
netrels=list() #List of tuples (parent, type, child, name)
existingRels = dict() #Key = (parent, type, child), val = rel id
existingProps = dict() #Keyed by node id + property name
nodeDescByName = dict() #dict of node descriptions keyed by name

appsList = list() # list of application ids
serverList = list() # list of server ids
busServicesList = list() # list of business services ids
dbServerList = list() #list of db ids
vmList = list() # list of VM ids
vcenterList = list() # list of vCenter ids
sanList = list() #List of storage kit
sanFabricList = list() #List of storage kit
sanStorageSwList = list() #List of Storage switches
dbList = list() # list of dbs
containerList = list() # list of storage containers
clusterList = list() # list of clusters
rackList = list() # list of racks
hwlbList = list() # list of HW loadbalancers
swlbList = list() # list of SW loadbalancers
netgearList = list() # list of network devices

newAppsList = list()
newServerList = list()
newBusServicesList = list()
newVMList = list()
newDatabaseList = list()
newSanList = list()
newSanFabricList = list()
newSanStorageSwList = list()
newContainerList = list()
newClusterList = list()
newHWLBList = list()
newSWLBList = list()
newNetGearList = list()

nodesFirstName = dict() #id keyed by nodes first word
hosts = dict()
servers = dict()
apps = dict()
devs = dict()
lpars = dict()
nets = dict()
buss = dict()


company = "NIE Networks"


	
def generateLine(id, columnList, outFile):
	name = nodesById.get(id)
	nodeProps = allPropsById[id]
	propsChangedSet = propsChanged.get(id, set())
	propsChangedSet.add(cmdbIdName) #ID always changed
	out = ''
	isNewToCmdb = name.lower() not in cmdb
	changed = isNewToCmdb #If new to CMDB then set Changed to true to ensure output line written
	count = 0
	#Iterate over the template file header cols. For each column, check if we have property set
	#If property set and it has changed, add it in otherwise write a blank value
	for col in columnList:
		if col == "Name": 
			val = name
		else:
			propName = propLookup.get(col, '')
			if propName not in propsChangedSet: val = ''
			else: 
				val = nodeProps.get(propName, '')
				if val == "Unknown": val = ''
				else:
					if propName != cmdbIdName: changed = True
		if count == 0: out = "%s" % val
		else: out = "%s,%s" % (out, val)
		count += 1
	#print out, changed
	if changed:	
		print >>outFile, out
	return changed

def exportAssets(outFile, templateFile, exportList):
	fdb = open(outFile, "w")
	fdbtemplate = open(templateFile)
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	entries = 0
	for id in exportList:
		if generateLine(id, cols, fdb): entries += 1
	fdb.close
	return entries
	
#Read in existing relationships
frels = open("relations.csv")
count = 0
lstr = ""
for line in frels:
	count += 1
	if count == 1: continue
	lstr += line.rstrip('\n\r')
	if lstr.endswith('"'):
		fs = lstr.split(",")
		relKey = (fs[4].strip('"'), fs[1].strip('"'),fs[5].strip('"'))
		existingRels[relKey] = fs[0].strip('"')
		lstr = ""
frels.close

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
		propKey = (id.strip('"'), name.strip('"'))
		existingProps[propKey] = val.strip('"')
		if id in allPropsById:
			allPropsById[id][name] = val
		else:
			allPropsById[id] = dict()
			allPropsById[id][name] = val
		if name == classPropName:
			if val == appStr: appsList.append(id)
			elif val == serverStr or val == esxServerStr or val == aixServerStr \
							or val == linuxStr or val == solarisStr or val == winStr or val == lparServerStr: serverList.append(id)
			elif val == vmwareStr or val == vmStr: vmList.append(id)
			elif val == vcenterStr: vcenterList.append(id)
			elif val == businessStr or val == busOfferStr: busServicesList.append(id)
			elif val == dbInstStr or val == dbSqlStr or val == dbOraStr or val == dbStr \
				or val == db2DbStr or val == mySqlDbStr or val == sybDbStr : dbList.append(id)
			elif val == sanSwitchStr: sanStorageSwList.append(id)
			elif val == sanFabricStr: sanFabricList.append(id)
			elif val == sanStr: sanList.append(id)
			elif val == storageServerStr: sanList.append(id)
			elif val == storageDevStr: sanList.append(id)
			elif val == containerStr: containerList.append(id)
			elif val == clusterStr: clusterList.append(id)
			elif val == rackStr: rackList.append(id)
			elif val == lbhwStr: hwlbList.append(id)
			elif val == lbswStr: swlbList.append(id)
			elif val == netgearStr  or val == fwStr: netgearList.append(id)
			elif val == subnetStr or val == groupStr: pass
			else: print "Not accounted for cmdb class: %s\n" % val
		lstr = ""
fprops.close
	
fcmdb = open("SNOW CMDB.csv")
count = 0
fullStr = ""
prevStr = False
cols = dict()
for lstr in fcmdb:
	count += 1
	if count == 1:
		cols = processHeader(lstr)
		#print cols
		for propName in propNameSet:
			col = cols.get(propRevLookup[propName], None)
			if col == None: 
				print "WARNING: Property %s not in CMDB export" % propRevLookup[propName]
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
	name = fields[1].lower()
	#if '#' in name: 
		#Names with this have been decommissioned, but not really in some cases
		#Only take the name as the bit before the #
	#	name = name.split('#')[0]
	
	cmdbId = fields[cols[propRevLookup[cmdbIdName]]]
	classField = fields[cols[propRevLookup[classPropName]]]
	status = fields[cols[propRevLookup[statusName]]]
	opStatus = fields[cols[propRevLookup[opStatusName]]].strip()
	#print cmdbId, classField, status, fields[cols['Updates']]
	#opStatus = 
	#if status == "Retired": continue
	#if opStatus == "Decommissioned": continue
	cmdbClass = ''
	if classField == "Computer" or classField == "Printer": continue
	if classField == appClass: cmdbClass = appStr
	elif classField == busServiceClass: cmdbClass = businessStr
	elif classField == busOfferingClass: cmdbClass = busOfferStr
	elif classField == serverClass: cmdbClass = serverStr
	elif classField == esxServerClass: cmdbClass = esxServerStr
	elif classField == aixServerClass: cmdbClass = aixServerStr
	elif classField == dbClass: cmdbClass = dbStr
	elif classField == dbInstClass: cmdbClass = dbInstStr
	elif classField == dbOraClass: cmdbClass = dbOraStr
	elif classField == dbSQLClass: cmdbClass = dbSqlStr
	elif classField == linuxClass: cmdbClass = linuxStr
	elif classField == solarisClass: cmdbClass = solarisStr
	elif classField == netClass: cmdbClass = netStr
	elif classField == winClass: cmdbClass = winStr
	elif classField == storageServerClass: cmdbClass = storageServerStr
	elif classField == storageDevClass: cmdbClass = storageDevStr
	elif classField == sanSwitchClass: cmdbClass = sanSwitchStr
	elif classField == sanFabricClass: cmdbClass = sanFabricStr
	elif classField == sanClass: cmdbClass = sanStr
	elif classField == containerClass: cmdbClass = containerStr
	elif classField == vmwareClass: cmdbClass = vmwareStr
	elif classField == vmClass: cmdbClass = vmStr
	elif classField == vcenterClass: cmdbClass = vcenterStr
	elif classField == clusterClass: cmdbClass = clusterStr
	elif classField == rackClass: cmdbClass = rackStr
	elif classField == lbhwClass: cmdbClass = lbhwStr
	elif classField == lbswClass: cmdbClass = lbswStr
	elif classField == fwClass: cmdbClass = fwStr
	else : 
		print "WARNING: (Snow read 1) CMDB name %s: Unrecognised CMDB class field: %s - ignoring entry" % (name, classField)
	if cmdbClass != '':
		cmdb[name] = cmdbId
		cmdbProps[(cmdbId, classPropName)] = cmdbClass
		for propName in propNameSet:
			if propName == classPropName: continue
			col = cols.get(propRevLookup[propName], None)
			if col != None: 
				val = fields[col].strip()
				if propName == manuName: val = val.strip("(Manufacturer)").strip()
				cmdbProps[(cmdbId, propName)] = val
		# ipAddr = fields[cols[propRevLookup[ipName]]].strip()
		# if ipAddr != '': 
			# cmdbProps[(cmdbId, ipName)] = ipAddr
			# subnet = ('0', '0', '0')
			# subnet = findSubnet(val)		
		# if status != '' : cmdbProps[(cmdbId, statusName)] = status
		# if opStatus != '' : cmdbProps[(cmdbId, opStatusName)] = opStatus
		# deviceType = fields[cols[propRevLookup[deviceTypeName]]].strip()
		# if deviceType != '': cmdbProps[(cmdbId, deviceTypeName)] = deviceType
		# funType = fields[cols[propRevLookup[fnName]]].strip()
		# if funType != '': cmdbProps[(cmdbId, fnName)] = funType
		# location = fields[cols[propRevLookup[locationName]]]
		# if location != '': cmdbProps[(cmdbId, locationName)] = location
		# model = fields[cols[propRevLookup[modelName]]].strip()
		# if model != '' : cmdbProps[(cmdbId, modelName)] = model
		# isMonitored = fields[cols[propRevLookup[isMonitoredName]]].strip()
		# if isMonitored != '' : cmdbProps[(cmdbId, isMonitoredName)] = isMonitored
		# monitoringObject = fields[cols[propRevLookup[monitorObName]]].strip()
		# if monitoringObject != '' : cmdbProps[(cmdbId, monitorObName)] = monitoringObject
		# monitoringTool = fields[cols[propRevLookup[monitorToolName]]].strip()
		# if monitoringTool != '' : cmdbProps[(cmdbId, monitorToolName)] = monitoringTool
		# serial = fields[cols[propRevLookup[serialName]]].strip()
		# if serial != '' : cmdbProps[(cmdbId, serialName)] = serial
		# assetTag = fields[cols[propRevLookup[assetTagName]]].strip()
		# if assetTag != '' : cmdbProps[(cmdbId, assetTagName)] = assetTag

fcmdb.close

print ("Archie export to Snow CMDB warnings")

#Read in Archie nodes from exported file
#If not in CMDB list, add to new list
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
	desc = ''
	for n in range(3, len(fs)): #Combine remaining fields - description field with commas
		if n != 3: desc += ',' #Add back in comma removed by split
		desc += fs[n]
	#nodesByName[name] = id
	nodesById[id] = name
	nodesByName[lowerName] = id
	nodeDescByName[name] = desc
	firstName = ''
	if nodeType == "Node" and "(" in name: 
		firstName = lowerName.split(" ")[0]
		nodesFirstName[firstName] = id
	if nodeType == "Node" and "." in name: 
		firstName = lowerName.split(".")[0]
		nodesFirstName[firstName] = id
	propsChanged[id] = set()
	archieStatus = existingProps.get((id, statusName), '').strip()
	archieRetired = archieStatus == "Retired" or archieStatus == "Absent" or archieStatus == "Disposed"
	if lowerName in cmdb: #Check props are the same - if not add to CMDB list to change
		cmdbId = cmdb[lowerName]
		cmdbStatus = cmdbProps.get((cmdbId, statusName), '').strip()
		cmdbRetired = cmdbStatus == "Retired" or cmdbStatus == "Absent" or cmdbStatus == "Disposed"
		if (cmdbRetired and not archieRetired) or (not cmdbRetired and archieRetired) or (not archieRetired and not cmdbRetired):
			for propName in propNameSet:
				archieVal = existingProps.get((id, propName), None)
				cmdbVal = cmdbProps.get((cmdbId, propName), None)
				if archieVal is None or cmdbVal is None: continue
				if archieVal.strip() != '' and archieVal.strip() != 'Unknown' and cmdbVal.strip() != archieVal.strip():
					print "%s has a changed property: %s (Archi = %s, CMDB = %s" % (name, propName, archieVal, cmdbVal)
					propsChanged[id].add(propName)
	else:
		#set all properties on new CI
		propsChanged[id] = propNameSet  
	if not archieRetired and (lowerName not in cmdb or len(propsChanged[id]) > 0):
		#Generate new or changed things
		#by adding to the correct new CI list
		if id in appsList: newAppsList.append(id)
		elif id in serverList: newServerList.append(id)
		elif id in vmList: newServerList.append(id)
		elif id in busServicesList: newBusServicesList.append(id)
		elif id in dbList: newDatabaseList.append(id)
		elif id in sanList: newSanList.append(id)
		elif id in sanStorageSwList: newSanStorageSwList.append(id)
		elif id in sanFabricList: newSanFabricList.append(id)
		elif id in containerList: newContainerList.append(id)
		elif id in clusterList: newClusterList.append(id)
		elif id in hwlbList: newHWLBList.append(id)
		elif id in swlbList: newSWLBList.append(id)
		elif id in netgearList: newNetGearList.append(id)
		#else: print "Missing id %s of class %s not handled\n" % (id, props.get(classPropName, '*CLASS NOT FOUND*'))
	
fnodes.close
#print "Existing nodes: " + str(count) + ": " + str(len(nodes))

#Load in subnets - can only be done once elements have been read
loadSubnets()

print "SNOW CMDB import to Archie warnings"
#Re-process CMDB export to determine what needs to be imported into Archie
fcmdb = open("SNOW CMDB.csv")
count = 0
lstr = ""
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
	fullStr = ''
	fields = csv.reader(csvList, delimiter=',', quotechar = '"').next()
	#fields = fullStr.rstrip('\n\r').split(",")
	cmdbId = fields[cols[propRevLookup[cmdbIdName]]].strip()
	if cmdbId == '':
		print "Ignoring empty or blank row (no cmdb Id)"
		continue
	#name = fields[1].strip().split('#')[0]
	name = fields[cols['Name']].strip()
	lowerName = name.lower()
	# if ".nie.co.uk" in lowerName: 
		# firstName = lowerName.split(".")[0]
	# else:
		# firstName = lowerName
	classField = fields[cols[propRevLookup[classPropName]]].strip()
	status = fields[cols[propRevLookup[statusName]]].strip()
	deviceType = fields[cols[propRevLookup[deviceTypeName]]].strip()
	funType = fields[cols[propRevLookup[fnName]]].strip()
	ipAddr = fields[cols[propRevLookup[ipName]]].strip()
	#print cmdbId, name, classField, status, fields[cols['Updates']]
	subnet = ('0', '0', '0')
	if ipAddr != '' : subnet = findSubnet(ipAddr)
	location = fields[cols[propRevLookup[locationName]]]
	manufacturer = fields[cols[propRevLookup[manuName]]].strip("(Manufacturer)").strip()
	model = fields[cols[propRevLookup[modelName]]].strip()
	isMonitored = fields[cols[propRevLookup[isMonitoredName]]].strip()
	monitoringObject = fields[cols[propRevLookup[monitorObName]]].strip()
	monitoringTool = fields[cols[propRevLookup[monitorToolName]]].strip()
	opStatus = fields[cols[propRevLookup[opStatusName]]].strip()
	serial = fields[cols[propRevLookup[serialName]]].strip()
	assetTag = fields[cols[propRevLookup[assetTagName]]].strip()
	
	#skip dev/test / retired / EUC
	if classField == computerClass or classField == printerClass : continue #Ignore all EUC devices
	#if status == "Retired": continue
	#if opStatus == "Decommissioned": continue
	if "#DECOM" in name: continue
	#if classField == appClass: continue #lots of system sofware in the CMDB, not just apps

	if classField == appClass: cmdbClass = appStr
	elif classField == busServiceClass: cmdbClass = businessStr
	elif classField == busOfferingClass: cmdbClass = busOfferStr
	elif classField == serverClass: cmdbClass = serverStr
	elif classField == esxServerClass: cmdbClass = esxServerStr
	elif classField == aixServerClass: cmdbClass = aixServerStr
	elif classField == dbClass: cmdbClass = dbStr
	elif classField == dbInstClass: cmdbClass = dbInstStr
	elif classField == dbOraClass: cmdbClass = dbOraStr
	elif classField == dbSQLClass: cmdbClass = dbSqlStr
	elif classField == linuxClass: cmdbClass = linuxStr
	elif classField == solarisClass: cmdbClass = solarisStr
	elif classField == netClass: cmdbClass = netStr
	elif classField == winClass: cmdbClass = winStr
	elif classField == storageServerClass: cmdbClass = storageServerStr
	elif classField == storageDevClass: cmdbClass = storageDevStr
	elif classField == sanSwitchClass: cmdbClass = sanSwitchStr
	elif classField == sanFabricClass: cmdbClass = sanFabricStr
	elif classField == sanClass: cmdbClass = sanStr
	elif classField == containerClass: cmdbClass = containerStr
	elif classField == vmwareClass: cmdbClass = vmwareStr
	elif classField == vmClass: cmdbClass = vmStr
	elif classField == vcenterClass: cmdbClass = vcenterStr
	elif classField == clusterClass: cmdbClass = clusterStr
	elif classField == rackClass: cmdbClass = rackStr
	elif classField == lbhwClass: cmdbClass = lbhwStr
	elif classField == lbswClass: cmdbClass = lbswStr
	elif classField == fwClass: cmdbClass = fwStr
	else : 
		print "WARNING: (Snow create) CMDB id %s, name %s: Unrecognised CMDB class field: %s - ignoring entry" % (cmdbId, name, classField)
		continue
#	if lowerName in nodesByName or lowerName in nodesFirstName or firstName in nodesByName: 
#	if lowerName in nodesByName or firstName in nodesByName: 
	archieStatus = existingProps.get((id, statusName), '').strip()
	archieRetired = archieStatus == "Retired" or archieStatus == "Absent" or archieStatus == "Disposed"
	cmdbRetired = status == "Retired" or status == "Absent" or status == "Disposed"
	if lowerName in nodesByName: 
		if (cmdbRetired and not archieRetired) or (not cmdbRetired and archieRetired) or (not archieRetired and not cmdbRetired):
			#Check all properties are set
			if lowerName in nodesByName: nodeId = nodesByName[lowerName]
			elif lowerName in nodesFirstName: nodeId = nodesFirstName[lowerName]
			else: nodeId = nodesByName[firstName]
			val = existingProps.get((nodeId, cmdbIdName), '')
			if cmdbId != val:
				if val != '':
					# print "Warning: %s Cmdb id has changed: from %s to %s - not changing" % (name, val, cmdbId)
					print "Warning: %s Cmdb id has changed: from %s to %s" % (name, val, cmdbId)
				# else:
				props[(nodeId, cmdbIdName)] = cmdbId
			val = existingProps.get((nodeId, classPropName), '')
			if cmdbClass != val:
				props[(nodeId, classPropName)] = cmdbClass
				if val != '':
					print "Warning: %s Cmdb class has changed: from %s to %s - not changing" % (name, val, cmdbClass)
				else:
					props[(nodeId, classPropName)] = cmdbClass
			val = existingProps.get((nodeId, locationName), '')
			if location != '' and location != 'Unknown' and location != val:
				props[(nodeId, locationName)] = location
			val = existingProps.get((nodeId, deviceTypeName), '')
			if deviceType != '' and deviceType != 'Unknown' and deviceType != val:
				props[(nodeId, deviceTypeName)] = deviceType
			val = existingProps.get((nodeId, fnName), '')
			if funType != '' and funType != 'Unknown' and funType != val:
				props[(nodeId, fnName)] = funType
			val = existingProps.get((nodeId, ipName), '')
			if ipAddr != '' and ipAddr != 'Unknown' and ipAddr != val:
				props[(nodeId, ipName)] = ipAddr
				rel = (nodeId, "AssociationRelationship", subnet[0])
				if subnet[0] != "0" and rel not in existingRels: 
					netrels.append((nodeId, "AssociationRelationship", subnet[0], ipAddr))
			val = existingProps.get((nodeId, manuName), '')
			if manufacturer != '' and manufacturer != 'Unknown' and manufacturer != val:
				props[(nodeId, manuName)] = manufacturer
			val = existingProps.get((nodeId, modelName), '')
			if model != '' and model != 'Unknown' and model != val:
				props[(nodeId, modelName)] = model
			val = existingProps.get((nodeId, serialName), '')
			if serial != '' and serial != 'Unknown' and serial != val:
				props[(nodeId, serialName)] = serial
			val = existingProps.get((nodeId, statusName), '')
			if status != '' and status != 'Unknown' and status != val:
				props[(nodeId, statusName)] = status
			val = existingProps.get((nodeId, isMonitoredName), '')
			if isMonitored != '' and isMonitored != val:
				props[(nodeId, isMonitoredName)] = isMonitored
			val = existingProps.get((nodeId, monitorObName), '')
			if monitoringObject != '' and monitoringObject != val:
				props[(nodeId, monitorObName)] = monitoringObject
			val = existingProps.get((nodeId, monitorToolName), '')
			if monitoringTool != '' and monitoringTool != val:
				props[(nodeId, monitorToolName)] = monitoringTool
			val = existingProps.get((nodeId, opStatusName), '')
			if opStatus != '' and opStatus != val:
				props[(nodeId, opStatusName)] = opStatus
			val = existingProps.get((nodeId, assetTagName), '')
			if assetTag != '' and assetTag != val:
				props[(nodeId, assetTagName)] = assetTag
	elif not cmdbRetired: 
		#Dont add ones that have been retired
		#print name, status
		dstr = "%s\n" % classField
		if deviceType != '': dstr += "Device Type: %s\n" % deviceType
		if model != '': dstr += "Model: %s\n" % model
		if manufacturer != '': dstr += "Manufacturer: %s\n" % manufacturer
		if serial != '': dstr += "Serial no: %s\n" % serial
		if ipAddr != '': dstr += "IP Address: %s\n" % ipAddr
		if location != '': dstr += "Location: %s\n" % location
		nodeId = str(uuid.uuid4())
		if classField == appClass: apps[name] = (nodeId, dstr.strip('"'))
		elif classField == busServiceClass or classField == busOfferingClass : 
			buss[name] = (nodeId, dstr.strip('"'))
		elif classField == serverClass: servers[name] = (nodeId, dstr.strip('"'))
		elif classField == esxServerClass or classField == aixServerClass \
							or classField == vmClass or classField == vmwareClass or classField == vcenterClass\
							or classField == winClass or classField == linuxClass:
			servers[name] = (nodeId, dstr.strip('"'))
		elif classField == dbClass: apps[name] = (nodeId, dstr.strip('"'))
		elif classField == netClass or classField == storageDevClass or classField == storageServerClass: devs[name] = (nodeId, dstr.strip('"'))
		else : 
			print "WARNING: (create) CMDB name %s: Unrecognised CMDB class field: %s - cannot add new node" % (name, classField)
			continue				
		nodesById[nodeId] = name  #Add to node array to resolve id to name
		props[(nodeId, classPropName)] = cmdbClass
		props[(nodeId, cmdbIdName)] = cmdbId
		if location != '': props[(nodeId, locationName)] = location
		if deviceType != '': props[(nodeId, deviceTypeName)] = deviceType
		if funType != '': props[(nodeId, fnName)] = funType
		if ipAddr != '': 
			props[(nodeId, ipName)] = ipAddr
			if subnet[0] != "0" :
				netrels.append((nodeId, "AssociationRelationship", subnet[0], ipAddr))
		if manufacturer != '' : props[(nodeId, manuName)] = manufacturer
		if model != '' : props[(nodeId, modelName)] = model
		if serial != '' : props[(nodeId, serialName)] = serial
		if status != '' : props[(nodeId, statusName)] = status
		if monitoringObject != '' : props[(nodeId, monitorObName)] = monitoringObject
		if monitoringTool != '' : props[(nodeId, monitorToolName)] = monitoringTool
		if opStatus != '' : props[(nodeId, opStatusName)] = opStatus
		if assetTag != '' : props[(nodeId, assetTagName)] = assetTag
fcmdb.close

if len(newAppsList) > 0:
	outFile = "new-cmdb-apps.csv"
	fapp = open(outFile, "w")
	fapptemplate = open("Application_Import_Template.csv")
	for t in fapptemplate:
		print >>fapp,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fapptemplate.close
	entries = 0
	for id in newAppsList:
		if generateLine(id, cols, fapp): entries += 1
	fapp.close
	if entries == 0: os.remove(outFile)
	else: print "%d Application entries" % entries

if len(newServerList) > 0:
	outFile = "new-cmdb-servers.csv"
	fserv = open(outFile, "w")
	fservtemplate = open("Server_Import_template.csv")
	for t in fservtemplate:
		print >>fserv,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fservtemplate.close
	entries = 0
	for id in newServerList:
		if generateLine(id, cols, fserv): entries += 1
	fserv.close
	if entries == 0: os.remove(outFile)
	else: print "%d Server entries" % entries

if len(newClusterList) > 0:
	outFile = "new-cmdb-cluster.csv"
	fbus = open(outFile, "w")
	fbustemplate = open("Cluster_Import_Template.csv")
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	entries = 0
	for id in newClusterList:
		if generateLine(id, cols, fbus): entries += 1
	fbus.close
	if entries == 0: os.remove(outFile)
	else: print "%d cluster entries" % entries

if len(newHWLBList) > 0:
	outFile = "new-cmdb-hw-loadbalancers.csv"
	fbus = open(outFile, "w")
	fbustemplate = open("HW_Load_Balancer_Import_Template.csv")
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	entries = 0
	for id in newHWLBList:
		if generateLine(id, cols, fbus): entries += 1
	fbus.close
	if entries == 0: os.remove(outFile)
	else: print "%d HW loadbalancers entries" % entries

if len(newSWLBList) > 0:
	outFile = "new-cmdb-sw-loadbalancers.csv"
	fbus = open(outFile, "w")
	fbustemplate = open("SW_Load_Balancer_Resource_Import_Template.csv")
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	entries = 0
	for id in newSWLBList:
		if generateLine(id, cols, fbus): entries += 1
	fbus.close
	if entries == 0: os.remove(outFile)
	else: print "%d SW loadbalancers entries" % entries

if len(newBusServicesList) > 0:
	outFile = "new-cmdb-service_offering.csv"
	fbus = open(outFile, "w")
	fbustemplate = open("Service_Offering_Import_Template.csv")
	entries = 0
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	for id in newBusServicesList:
		if generateLine(id, cols, fbus): entries += 1
	fbus.close
	if entries == 0: os.remove(outFile)
	else: print "%d Business Service Offering entries" % entries

if len(newDatabaseList) > 0:
	outFile = "new-cmdb-database-instances.csv"
	fdb = open(outFile, "w")
	fdbtemplate = open("Database_Instance_Import_Template.csv")
	entries = 0
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	for id in newDatabaseList:
		if generateLine(id, cols, fdb): entries += 1
	fdb.close
	if entries == 0: os.remove(outFile)
	else: print "%d Database entries" % entries

# fdb = open("new-cmdb-databases.csv", "w")
# fdbtemplate = open("Database_Import_Template.csv")
# for t in fdbtemplate:
	# print >>fdb,t
# fdbtemplate.close
# for dbId in newDatabaseList:
	# db = nodesById.get(dbId)
	# props = allPropsById[dbId]
	# cmdbId = props.get(cmdbIdName, '')
	# dbClass = props.get(classPropName, '')
	# type = ''
	# if dbClass == dbOraStr: type = "Oracle"
	# elif dbClass == dbSqlStr: type = "Microsoft SQL Server"
	# criticality = props.get(criticalityName, '')
	# installPath = props.get(installName, '')
	
	# print >>fdb, "%s,%s,%s,%s,,01/10/2017,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s,,,,,,,,,,,\"%s\",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s," \
										# % (cmdbId, db, company, criticality, nodeDescByName[db], installPath, type)
# fdb.close

if len(newContainerList) > 0:
	outFile = "new-cmdb-containers.csv"
	fcont = open(outFile, "w")
	ftemp = open("Storage_Container_Import_Template.csv")
	for t in ftemp:
		print >>fcont,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	ftemp.close
	entries = 0
	for id in newContainerList:
		if generateLine(id, cols, fcont): entries += 1
	fcont.close
	if entries == 0: os.remove(outFile)
	else: print "%d Container entries" % entries

if len(newSanList) > 0:
	outFile = "new-cmdb-san.csv"
	fdb = open(outFile, "w")
	fdbtemplate = open("SAN_Import_Template.csv")
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	entries = 0
	for id in newSanList:
		if generateLine(id, cols, fdb): entries += 1
	fdb.close
	if entries == 0: os.remove(outFile)
	else: print "%d SAN entries" % entries

if len(newSanStorageSwList) > 0:
	outFile = "new-cmdb-storage-switch.csv"
	fdb = open(outFile, "w")
	fdbtemplate = open("Storage_Switch_Import_Template.csv")
	cols = list()
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	entries = 0
	for id in newSanStorageSwList:
		if generateLine(id, cols, fdb): entries += 1
	fdb.close
	if entries == 0: os.remove(outFile)
	else: print "%d SAN Storage entries" % entries

if len(newSanFabricList) > 0:
	outFile = "new-cmdb-sanfabric.csv"
	templateFile = "SAN_Fabric_Import_Template.csv"
	entries = exportAssets(outFile, templateFile, newSanFabricList)
	if entries == 0: os.remove(outFile)
	else: print "%d SAN Fabric entries" % entries

if len(newNetGearList) > 0:
	outFile = "new-cmdb-netgear.csv"
	templateFile = "Network_Gear_Import_Template.csv"
	entries = exportAssets(outFile, templateFile, newNetGearList)
	if entries == 0: os.remove(outFile)
	else: print "%d Net Gear entries" % entries
	
#Now update archie
felems = open("new-elements.csv", "w")
print >>felems,'"ID","Type","Name","Documentation"'
for h in hosts:
	print >>felems,'"%s","Node","%s","%s"' % (hosts[h][0],h,hosts[h][1])
for s in servers:
	print >>felems,'"%s","Node","%s","%s"' % (servers[s][0],s, servers[s][1])
for a in apps:
	print >>felems,'"%s","ApplicationComponent","%s","%s"' % (apps[a][0], a, apps[a][1])
#for net in nets:
#	print >>felems,'"%s","CommunicationNetwork","%s","%s"' % (nets[net][0], net, nets[net][1])
for d in devs:
	print >>felems,'"%s","Device","%s","%s"' % (devs[d][0], d, devs[d][1])
for b in buss:
	print >>felems,'"%s","BusinessService","%s","%s"' % (buss[b][0], b, buss[b][1])
	
felems.close

frels = open("new-relations.csv", "w")
freadable = open("new-relations-readable.csv", "w")
print >>frels,'"ID","Type","Name","Documentation","Source","Target"'
print >>freadable,'"Parent","Child","Relationship"'
for rel in rels:
	print >>frels, '"","%s","","","%s","%s"' % (rel[1], rel[0], rel[2])
	print >>freadable, '"%s","%s","%s"' % (nodesById[rel[0]], nodesById[rel[2]], rel[1])
frels.close	

fprops = open("new-properties.csv", "w")
fread = open("new-properties-readable.csv", "w")
print >>fprops,'"ID","Key","Value"'
print >>fread,'"Name","Key","Value"'
for prop in props:
	print >>fprops, '"%s","%s","%s"' % (prop[0], prop[1], props[prop])
	print >>fread, '"%s","%s","%s"' % (nodesById[prop[0]], prop[1], props[prop])
fprops.close
fread.close
