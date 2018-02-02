#Snow CMDB synchronisation with Archie - Archie import
#Compares CMDB export with Archie export and creates both Archie and CMDB import entries
#Author: Danny Andersen

import sys
import uuid
import csv

#nodesByName = dict() #Keyed by node name, id of node
nodesById = dict() # Keyed by node id, name of node
nodes = dict() #id keyed by name
cmdb = dict() #Keyed by Name, the cmdb class
props = dict() #New props, keyed by node id + property name
propsChanged = dict() #Keyed by id, set of all properties that have changed
allPropsById = dict() #dict of dict of all properties found for a particular element keyed by its id  and the prop name
rels=list() #List of tuples (parent, type, child, name)
netrels=list() #List of tuples (parent, type, child, name)
existingRels = dict() #Key = (parent, type, child), val = rel id
existingProps = dict() #Keyed by node id + property name
nodeDescByName = dict() #dict of node descriptions keyed by name
cmdbProps = dict() #CMDB props from CMDB file, keyed by cmdb id + property name

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

nodesFirstName = dict() #id keyed by nodes first word
hosts = dict()
servers = dict()
apps = dict()
devs = dict()
lpars = dict()
nets = dict()
buss = dict()
subnets = list()

cmdbIdName = "CMDB ID"
classPropName = "CMDB Class"
deviceTypeName = "CMDB Device Type"
osName = "CMDB Operating System"
fnName = "CMDB Function"
ipName = "CMDB IP Address"
manuName = "CMDB Manufacturer"
modelName = "CMDB Model"
locationName = "CMDB Location"
criticalityName = "CMDB Criticality"
serviceClassName = "CMDB Service Classification"
installName = "CMDB Installation Path"
statusName = "CMDB Status"
serialName = "CMDB Serial"
opStatusName = "CMDB Operational Status"
monitorObName = "CMDB Monitoring Object Id"
monitorToolName = "CMDB Monitoring Tool"
isMonitoredName	= "CMDB IsMonitored"
domainName = "CMDB Domain DNS"

propNameSet = {classPropName, deviceTypeName, osName, fnName, ipName, manuName, modelName, \
					locationName,criticalityName,serviceClassName, installName, \
					statusName, serialName, opStatusName, domainName, \
					monitorObName, monitorToolName, isMonitoredName}

propLookup = {"Unique ID": cmdbIdName, "Class": classPropName, "Device Type": deviceTypeName, \
					osName: "OS Version", "Function Type": fnName, \
					"IP Address": ipName,  "Manufacturer": manuName, "Model ID": modelName, \
					"Location": locationName, "Criticality": criticalityName, "Service classification": serviceClassName, \
					"Installed": installName, "Status": statusName, "Serial number": serialName, \
					"Operational status": opStatusName, "DNS Domain": domainName, \
					"Monitoring Object ID": monitorObName, "Monitoring Tool": monitorToolName, "Is Monitored": isMonitoredName }
					
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

company = "NIE Networks"

def inSubnet(subnet, mask, ipToCheck):
	if (subnet == '' or mask == '' or ipToCheck == ''):
		print "Empty field, cant look for subnet for IP: %s, %s, %s" % (subnet,mask,ipToCheck)
		return False
	subs = subnet.split('.')
	mk = mask.split('.')
	ip = ipToCheck.split('.')
	#print subnet, mask, ipToCheck
	return (int(ip[0]) & int(mk[0]) == int(subs[0])) and (int(ip[1]) & int(mk[1]) == int(subs[1])) and int(ip[2]) & int(mk[2]) == int(subs[2]) and int(ip[3]) & int(mk[3]) == int(subs[3])

def findSubnet(ipAddr):
	ret = ("0", "0", "0")
	for id, subnet, mask in subnets:
		if inSubnet(subnet, mask, ipAddr): 
			ret = (id, subnet, mask)
			break;
	return ret
	

#Process header line and return a dict keyed by column name, with value of field number	
def processHeader(headerLine):
	cols = headerLine.strip('\n\r').split(',')
	colDict = dict()
	num = 0;
	for col in cols:
		colDict[col.strip()] = num
		num += 1
	return colDict

#Process header line and return a of each header	
def getPropList(headerLine):
	cols = headerLine.strip('\n\r').split(',')
	return cols

def generateLine(id, columnList, outFile):
	name = nodesById.get(id)
	nodeProps = allPropsById[id]
	propsChangedSet = propsChanged.get(id, set())
	propsChangedSet.add(cmdbIdName) #ID always changed
	#Iterate over the template file header cols. For each column, check if we have property set
	#If property set and it has changed, add it in otherwise write a blank value
	out = ''
	changed = False
	count = 0
	for col in columnList:
		if col == "Name": val = name
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
	if changed:	print >>outFile, out
	
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
			elif val == netgearStr or val == subnetStr or val == groupStr: pass
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
	
	cmdbId = fields[cols['Unique ID']]
	classField = fields[cols['Class']]
	status = fields[cols['Status']]
	#print cmdbId, classField, status, fields[cols['Updates']]
	#opStatus = 
	if status == "Retired": continue
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
	else : 
		print "WARNING: (Snow read 1) CMDB name %s: Unrecognised CMDB class field: %s - ignoring entry" % (name, classField)
	if cmdbClass != '':
		cmdb[name] = cmdbId
		classField = fields[cols['Class']].strip()
		status = fields[cols['Status']].strip()
		deviceType = fields[cols['Device Type']].strip()
		funType = fields[cols['Function Type']].strip()
		ipAddr = fields[cols['IP Address']].strip()
		subnet = ('0', '0', '0')
		if ipAddr != '' : subnet = findSubnet(ipAddr)
		location = fields[cols['Location']]
		manufacturer = fields[cols['Manufacturer']].strip("(Manufacturer)").strip()
		model = fields[cols['Model ID']].strip()
		isMonitored = fields[cols['Is Monitored']].strip()
		monitoringObject = fields[cols['Monitoring Object ID']].strip()
		monitoringTool = fields[cols['Monitoring Tool']].strip()
		opStatus = fields[cols['Operational status']].strip()
		serial = fields[cols['Serial number']].strip()
		cmdbProps[(cmdbId, classPropName)] = cmdbClass
		if location != '': cmdbProps[(cmdbId, locationName)] = location
		if deviceType != '': cmdbProps[(cmdbId, deviceTypeName)] = deviceType
		if funType != '': cmdbProps[(cmdbId, fnName)] = funType
		if ipAddr != '': 
			cmdbProps[(cmdbId, ipName)] = ipAddr
		if manufacturer != '' : cmdbProps[(cmdbId, manuName)] = manufacturer
		if model != '' : cmdbProps[(cmdbId, modelName)] = model
		if serial != '' : cmdbProps[(cmdbId, serialName)] = serial
		if status != '' : cmdbProps[(cmdbId, statusName)] = status
		if isMonitored != '' : cmdbProps[(cmdbId, isMonitoredName)] = isMonitored
		if monitoringObject != '' : cmdbProps[(cmdbId, monitorObName)] = monitoringObject
		if monitoringTool != '' : cmdbProps[(cmdbId, monitorToolName)] = monitoringTool
		if opStatus != '' : cmdbProps[(cmdbId, opStatusName)] = opStatus

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
	nodes[lowerName] = id
	nodeDescByName[name] = desc
	firstName = ''
	if nodeType == "Node" and "(" in name: 
		firstName = lowerName.split(" ")[0]
		nodesFirstName[firstName] = id
	if nodeType == "Node" and "." in name: 
		firstName = lowerName.split(".")[0]
		nodesFirstName[firstName] = id
	propsChanged[id] = set()
	if lowerName in cmdb: #Check props are the same - if not add to CMDB list to change
		cmdbId = cmdb[lowerName]
		for propName in propNameSet:
			archieVal = existingProps.get((id, propName), '').strip()
			cmdbVal = cmdbProps.get((cmdbId, propName))
			if cmdbVal != None and archieVal != '' and archieVal != 'Unknown' and cmdbVal.strip() != archieVal:
				print "%s has a changed property: %s (Archi = %s, CMDB = %s" % (name, propName, archieVal, cmdbVal)
				propsChanged[id].add(propName)
	else:
		propsChanged[id] = propNameSet  #set all properties on new CI
	if lowerName not in cmdb or len(propsChanged[id]) > 0: #Generate new or changed things
		#Add to the correct new CI list
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
		#else: print "Missing id %s of class %s not handled\n" % (id, props.get(classPropName, '*CLASS NOT FOUND*'))
	
fnodes.close
#print "Existing nodes: " + str(count) + ": " + str(len(nodes))

#TODO: Replace with using existing properties to set up subnets
fh = open("subnets.csv", "r")
prevStr = False
for line in fh:
	lstr = line
	if lstr.count('"') == 1:
		#Multi-line entry
		#print "Multi"
		if not prevStr:
			#First line
			prevStr = True
			while lstr.rfind(',') > lstr.rfind('"'):
				#Remove commas in text
				r = lstr.rfind(',')
				lstr = lstr[0:r] + lstr[r+1:len(lstr)]
			fullStr += lstr
			#print "current str: %s, fullStr: %s" % (lstr,fullStr)
			continue
		fullStr += lstr
	elif prevStr:
		#Continuing line, remove any commas
		fullStr += lstr.replace(',',' ')
		continue
	else :
		fullStr = lstr
	prevStr = False
	#Remove commas in quoted strings
	if fullStr.count('"') == 2:
		left = fullStr[0:fullStr.find('"')]
		right = fullStr[fullStr.rfind('"') + 1:len(fullStr)]
		quoted = fullStr[fullStr.find('"'):fullStr.rfind('"')+1]
		#print "bits: left:%s right:%s quoted:%s" % (left,right,quoted)
		fullStr = left + quoted.replace(',','') + right
		#print "cleaned: " + fullStr
	fs = fullStr.rstrip('\n\r').split(",")
	fullStr = ''
	subnet = fs[1].split('/')
	desc = fs[2].strip('"')
	site = fs[3]
	#Skip if no description, as not used
	if desc == '' : continue
	baseAddr = subnet[0]
	bits = int(subnet[1])
	mask = "0.0.0.0"
	if bits >= 24:
		topBits = 8 - (32 - bits)
		m = 0
		for i in range(7, 7-topBits,-1) :
			m += 1 << i
		mask = "255.255.255.%d" % m
		#print "Bits %d == mask %s" % (bits, mask)
	elif bits == 16:
		mask = "255.255.0.0"
	elif bits == 8:
		mask = "255.0.0.0"
	else :
		print "Cant calculate bitmask for subnet %s" % (fs[1])
	#print "Subnet %s - mask: %s" % (subnet, mask)
	#Skip if subnet already in VLAN table
	skip = False
	for id, sub, m in subnets:
		if sub == baseAddr and m == mask :
			skip = True
			break
	if skip : continue
	#Skip if already set in elements file
	if baseAddr.lower() in nodes :
		#print "subnet %s already exists" % subnet
		baseAddr = baseAddr.lower()
		subnets.append((nodes[baseAddr], baseAddr, mask))
	else:
		print "WARNING - found new subnet???? %s" % baseAddr
	
fh.close

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
	cmdbId = fields[cols['Unique ID']].strip()
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
	classField = fields[cols['Class']].strip()
	status = fields[cols['Status']].strip()
	deviceType = fields[cols['Device Type']].strip()
	funType = fields[cols['Function Type']].strip()
	ipAddr = fields[cols['IP Address']].strip()
	#print cmdbId, name, classField, status, fields[cols['Updates']]
	subnet = ('0', '0', '0')
	if ipAddr != '' : subnet = findSubnet(ipAddr)
	location = fields[cols['Location']]
	manufacturer = fields[cols['Manufacturer']].strip("(Manufacturer)").strip()
	model = fields[cols['Model number']].strip()
	isMonitored = fields[cols['Is Monitored']].strip()
	monitoringObject = fields[cols['Monitoring Object ID']].strip()
	monitoringTool = fields[cols['Monitoring Tool']].strip()
	opStatus = fields[cols['Operational status']].strip()
	serial = fields[cols['Serial number']].strip()
	#skip dev/test / retired / EUC
	if classField == computerClass or classField == printerClass : continue #Ignore all EUC devices
	if status == "Retired": continue
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
	else : 
		print "WARNING: (Snow create) CMDB id %s, name %s: Unrecognised CMDB class field: %s - ignoring entry" % (cmdbId, name, classField)
		continue
#	if lowerName in nodes or lowerName in nodesFirstName or firstName in nodes: 
#	if lowerName in nodes or firstName in nodes: 
	if lowerName in nodes: 
		#Check all properties are set
		if lowerName in nodes: nodeId = nodes[lowerName]
		elif lowerName in nodesFirstName: nodeId = nodesFirstName[lowerName]
		else: nodeId = nodes[firstName]
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
	else :
		#if name.startswith("NIE-CTX") : continue #Cytrix nodes are wrong currently
		if status == "Retired" or status == "Absent" : continue
		#if classField == appClass:
		#	print "WARNING: Ignoring application %s" % name
		#	continue #lots of system sofware in the CMDB, not just apps
		#Skip creating dev/test for now
		#if lowerName.startswith("nied") : continue
		#if lowerName.startswith("nie-drg"): continue
		#if lowerName.startswith("nie-dg") : continue
		#if lowerName.startswith("esd-nie"): continue
		#if lowerName.startswith("esd-ctx"): continue
		#if lowerName.startswith("billing2"): continue
		#if lowerName.startswith("redhat2"): continue
		#if "test" in lowerName: continue
		#if "tst" in lowerName: continue
		#if "dev" in lowerName: continue
		#if "Dargan" in location: continue
		#if (classField == serverClass or classField == aixServerClass) and "(" in name: continue #ignore servers with ( in name
		#Create node
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
fcmdb.close

if len(newAppsList) > 0:
	fapp = open("new-cmdb-apps.csv", "w")
	fapptemplate = open("Application_Import_Template.csv")
	for t in fapptemplate:
		print >>fapp,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fapptemplate.close
	for appId in newAppsList:
		generateLine(appId, cols, fapp)
	fapp.close

if len(newServerList) > 0:
	fserv = open("new-cmdb-servers.csv", "w")
	fservtemplate = open("Server_Import_template.csv")
	for t in fservtemplate:
		print >>fserv,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fservtemplate.close
	for serverId in newServerList:
		generateLine(serverId, cols, fserv)
	fserv.close

if len(newClusterList) > 0:
	fbus = open("new-cmdb-cluster.csv", "w")
	fbustemplate = open("Cluster_Import_Template.csv")
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	for id in newClusterList:
		generateLine(id, cols, fbus)
	fbus.close

if len(newHWLBList) > 0:
	fbus = open("new-cmdb-hw-loadbalancers.csv", "w")
	fbustemplate = open("HW_Load_Balancer_Import_Template.csv")
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	for id in newHWLBList:
		generateLine(id, cols, fbus)
	fbus.close

if len(newSWLBList) > 0:
	fbus = open("new-cmdb-sw-loadbalancers.csv", "w")
	fbustemplate = open("SW_Load_Balancer_Resource_Import_Template.csv")
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	for id in newSWLBList:
		generateLine(id, cols, fbus)
	fbus.close

if len(newBusServicesList) > 0:
	fbus = open("new-cmdb-service_offering.csv", "w")
	fbustemplate = open("Service_Offering_Import_Template.csv")
	for t in fbustemplate:
		print >>fbus,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fbustemplate.close
	for busId in newBusServicesList:
		generateLine(busId, cols, fbus)
	fbus.close

if len(newDatabaseList) > 0:
	fdb = open("new-cmdb-databases.csv", "w")
	fdbtemplate = open("Database_Instance_Import_Template.csv")
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	for dbId in newDatabaseList:
		generateLine(dbId, cols, fdb)
	fdb.close

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
	fcont = open("new-cmdb-containers.csv", "w")
	ftemp = open("Storage_Container_Import_Template.csv")
	for t in ftemp:
		print >>fcont,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	ftemp.close
	for id in newContainerList:
		generateLine(id, cols, fcont)
	fcont.close

if len(newSanList) > 0:
	fdb = open("new-cmdb-san.csv", "w")
	fdbtemplate = open("SAN_Import_Template.csv")
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	for sanId in newSanList:
		generateLine(sanId, cols, fdb)
	fdb.close

if len(newSanStorageSwList) > 0:
	fdb = open("new-cmdb-storage-switch.csv", "w")
	fdbtemplate = open("Storage_Switch_Import_Template.csv")
	cols = list()
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	for sanId in newSanStorageSwList:
		generateLine(sanId, cols, fdb)
	fdb.close

if len(newSanFabricList) > 0:
	fdb = open("new-cmdb-sanfabric.csv", "w")
	fdbtemplate = open("SAN_Fabric_Import_Template.csv")
	for t in fdbtemplate:
		print >>fdb,t
		cols = getPropList(t)
		if len(cols) > 0: break #Got header
	fdbtemplate.close
	for sanId in newSanFabricList:
		generateLine(sanId, cols, fdb)
	fdb.close

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
print >>frels,'"ID","Type","Name","Documentation","Source","Target"'
for rel in rels:
	print >>frels, '"","%s","","","%s","%s"' % (rel[1], rel[0], rel[2])
for rel in netrels:
	print >>frels, '"","%s","%s","","%s","%s"' % (rel[1], rel[3], rel[0], rel[2])
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
