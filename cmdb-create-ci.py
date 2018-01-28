#Archie EA tool to Snow CMDB synchronisation script
#Compares CMDB export with Archie export and create entries in the relevant CMDB import file
#Author: Danny Andersen

import sys
import uuid

#nodesByName = dict() #Keyed by node name, id of node
nodesById = dict() # Keyed by node id, name of node
cmdb = dict() #Keyed by Name, the cmdb class
props = dict() #Keyed by node id + property name
allPropsById = dict() #dict of dict of all properties found for a particular element keyed by its id  and the prop name
nodeDescByName = dict() #dict of node descriptions keyed by name

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
newContainerList = list()
newClusterList = list()
newHWLBList = list()
newSWLBList = list()

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

fcmdb = open("SNOW CMDB.csv")
count = 0
for line in fcmdb:
	count += 1
	if count == 1: continue
	fields=line.rstrip("\n\r").split(",")
	name = fields[1].lower()
	# if '#' in name: 
		# #Names with this have been decommissioned, but not really in some cases
		# #Only take the name as the bit before the #
		# name = name.split('#')[0]
	cmdbId = fields[0]
	classField = fields[2]
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
		print "WARNING: CMDB name %s: Unrecognised CMDB class field: %s - ignoring entry" % (name, classField)
		continue
	cmdb[name] = cmdbClass
	print name
fcmdb.close

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
		#existingProps[propKey] = val.strip('"')
		if id in allPropsById:
			allPropsById[id][name] = val
		else:
			allPropsById[id] = dict()
			allPropsById[id][name] = val
		if name == classPropName:
			if val == appStr: appsList.append(id)
			elif val == serverStr or val == esxServerStr or val == aixServerStr \
							or val == linuxStr or val == winStr or val == lparServerStr: serverList.append(id)
			elif val == vmwareStr: vmList.append(id)
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
			elif val == clusterStr: clusterList.append(id)
			elif val == rackStr: rackList.append(id)
			elif val == lbhwStr: hwlbList.append(id)
			elif val == lbswStr: swlbList.append(id)
			elif val == netgearStr or val == subnetStr or val == groupStr: pass
			else: print "Not accounted for cmdb class: %s\n" % val
		lstr = ""
fprops.close

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
	fs = fullStr.rstrip('\n\r').split(",")
	fullStr = ''
	#print fs[0], fs[1], fs[2], fs[3]
	name = fs[2].strip('"')
	id = fs[0].strip('"')
	desc = ''
	for n in range(3, len(fs)): #Combine remaining fields - description field with commas
		if n != 3: desc += ',' #Add back in comma removed by split
		desc += fs[n]
	#nodesByName[name] = id
	nodesById[id] = name
	nodeDescByName[name] = desc
	name = name.lower()
	if name not in cmdb: #Only generate new things
		#Add to the correct new CI list
		if id in appsList: newAppsList.append(id)
		elif id in serverList: newServerList.append(id)
		elif id in vmList: newServerList.append(id)
		elif id in busServicesList: newBusServicesList.append(id)
		elif id in dbList: newDatabaseList.append(id)
		elif id in sanList: newSanList.append(id)
		elif id in sanFabricList: newSanFabricList.append(id)
		elif id in containerList: newContainerList.append(id)
		elif id in clusterList: newClusterList.append(id)
		elif id in hwlbList: newHWLBList.append(id)
		elif id in swlbList: newSWLBList.append(id)
		#else: print "Missing id %s of class %s not handled\n" % (id, props.get(classPropName, '*CLASS NOT FOUND*'))
fnodes.close
#print "Existing nodes: " + str(count) + ": " + str(len(nodes))

fapp = open("new-cmdb-apps.csv", "w")
fapptemplate = open("Application_Import_Template.csv")
for t in fapptemplate:
	print >>fapp,t
fapptemplate.close
for appId in newAppsList:
	app = nodesById.get(appId)
	props = allPropsById[appId]
	cmdbId = props.get(cmdbIdName, '')
	print >>fapp, "%s,%s,%s,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s," % (cmdbId, app, company,nodeDescByName[app])
fapp.close

fserv = open("new-cmdb-servers.csv", "w")
fservtemplate = open("Server_Import_template.csv")
for t in fservtemplate:
	print >>fserv,t
fservtemplate.close
for serverId in newServerList:
	name = nodesById.get(serverId)
	props = allPropsById[serverId]
	cls = props.get(classPropName, '')
	cmdbId = props.get(cmdbIdName, '')
	#if cls == lparServerStr: cls = aixServerStr  #Convert class to AIX rather than the mainframe lpar
	model = props.get(modelName, '')
	if model == "Unknown": model = ''
	manufacturer = props.get(manuName, '')
	if manufacturer == "Unknown": manufacturer = ''
	fn = props.get(fnName, '')
	if fn == "Unknown": fn = ''
	location = props.get(locationName, '')
	if location == "Unknown": location = ''
	ipAddress = props.get(ipName, '')
	if ipAddress == "Unknown": ipAddress = ''
	os = props.get(osName, '')
	if os == "Unknown": os = ''
	status = props.get(statusName, '')
	devType = props.get(deviceTypeName, '')
	if devType == "Unknown": devType = ''
	serial = props.get(serialName, '')
	if serial == "Unknown": serial = ''
	osFamily = ''
	oslower = os.lower()
	if "windows" in oslower: osFamily = "Windows"
	elif "linux" in oslower: osFamily = "Linux"
	elif "aix" in oslower or "unix" in oslower or "sun" in oslower: osFamily = "Unix"
	elif "esx" in oslower: osFamily = "Proprietary"
	print >>fserv, "%s,%s,%s,%s,,,,,,,,,,,%s,%s,,,%s,%s,,%s,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s,%s,,,,,,,,,,,,,,,,,,%s,,,,,,,,,,,%s,,,,,,,,,,,,,,,%s,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s," \
							% (cmdbId,name,cls,company,location, model, osFamily, os, serial, nodeDescByName[name],devType,fn, ipAddress,manufacturer,status)
fserv.close

fbus = open("new-cmdb-cluster.csv", "w")
fbustemplate = open("Cluster_Import_Template.csv")
for t in fbustemplate:
	print >>fbus,t
fbustemplate.close
for id in newClusterList:
	name = nodesById.get(id)
	props = allPropsById[id]
	cmdbId = props.get(cmdbIdName, '')
	criticality = props.get(criticalityName, '')
	status = props.get(statusName, '')
	opStatus = props.get(opStatusName, 'Live')
	serviceClass = props.get(serviceClassName, '')
	fn = props.get(fnName, '')
	if fn == "Unknown": fn = ''
	devType = props.get(deviceTypeName, '')
	ipAddress = props.get(ipName, '')
	if ipAddress == "Unknown": ipAddress = ''
	print >>fbus, "%s,%s,%s,%s,,,,,,,,,,,,,,,,,,,,,,,,,%s,,,,,,%s,%s,,,,,,,,%s,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s" \
							% (cmdbId, name, company, opStatus, criticality, nodeDescByName[name], devType, fn,status)
fbus.close

fbus = open("new-cmdb-hw-loadbalancers.csv", "w")
fbustemplate = open("HW_Load_Balancer_Import_Template.csv")
for t in fbustemplate:
	print >>fbus,t
fbustemplate.close
for id in newHWLBList:
	name = nodesById.get(id)
	props = allPropsById[id]
	cmdbId = props.get(cmdbIdName, '')
	criticality = props.get(criticalityName, '').strip()
	status = props.get(statusName, '').strip()
	opStatus = props.get(opStatusName, 'Live').strip()
	serviceClass = props.get(serviceClassName, '')
	fn = props.get(fnName, '').strip()
	if fn == "Unknown": fn = ''
	devType = props.get(deviceTypeName, '')
	model = props.get(modelName, '').strip()
	if model == "Unknown": model = ''
	serial = props.get(serialName, '').strip()
	if serial == "Unknown": serial = ''
	location = props.get(locationName, '').strip()
	if location == "Unknown": location = ''
	ipAddress = props.get(ipName, '').strip()
	if ipAddress == "Unknown": ipAddress = ''
	manufacturer = props.get(manuName, '').strip()
	if manufacturer == "Unknown": manufacturer = ''
	print >>fbus, "%s,%s,%s,%s,,,,,,,,,,%s,,,%s,,%s,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s,%s,,,,,,,,,,,%s,,,,,,,,%s,,,,,,,,,,,,,,,,,,%s,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s" \
							% (cmdbId, name, company, criticality, model, opStatus, serial, nodeDescByName[name], devType, fn,ipAddress, manufacturer,status)
fbus.close

fbus = open("new-cmdb-sw-loadbalancers.csv", "w")
fbustemplate = open("SW_Load_Balancer_Resource_Import_Template.csv")
for t in fbustemplate:
	print >>fbus,t
fbustemplate.close
for id in newSWLBList:
	cmdbId = props.get(cmdbIdName, '')
	name = nodesById.get(id)
	props = allPropsById[id]
	status = props.get(statusName, '').strip()
	opStatus = props.get(opStatusName, 'Live').strip()
	cls = props.get(classPropName, '')
	print >>fbus, "%s,%s,%s,,%s,,,,,,,,%s,,,,,,,%s,,,,,,,,,,,,%s" \
							% (cmdbId, name, company, cls, nodeDescByName[name], status, opStatus)
fbus.close

fbus = open("new-cmdb-service_offering.csv", "w")
fbustemplate = open("Service_Offering_Import_Template.csv")
for t in fbustemplate:
	print >>fbus,t
fbustemplate.close
for busId in newBusServicesList:
	bus = nodesById.get(busId)
	props = allPropsById[busId]
	cmdbId = props.get(cmdbIdName, '')
	criticality = props.get(criticalityName, '')
	status = props.get(statusName, '')
	serviceClass = props.get(serviceClassName, '')
	print >>fbus, "%s,%s,%s,,,,,,,,,,,,,,,,,,,%s,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s,,,,,%s" \
							% (cmdbId, bus, company, criticality, serviceClass, status)
fbus.close

fdb = open("new-cmdb-databases.csv", "w")
fdbtemplate = open("Database_Instance_Import_Template.csv")
for t in fdbtemplate:
	print >>fdb,t
fdbtemplate.close
for dbId in newDatabaseList:
	db = nodesById.get(dbId)
	props = allPropsById[dbId]
	cmdbId = props.get(cmdbIdName, '')
	dbClass = props.get(classPropName, '')
	# type = ''
	# if dbClass == dbOraStr: type = "Oracle"
	# elif dbClass == dbSqlStr: type = "Microsoft SQL Server"
	criticality = props.get(criticalityName, '')
	installPath = props.get(installName, '')
	
	print >>fdb, "%s,%s,%s,%s,%s,,01/10/2017,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,%s,,,,,,,,,,,,,,,,\"%s\"," \
										% (cmdbId, dbClass, db, company, criticality, nodeDescByName[db], installPath)
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

fcont = open("new-cmdb-containers.csv", "w")
ftemp = open("Storage_Container_Import_Template.csv")
for t in ftemp:
	print >>fcont,t
ftemp.close
for id in newContainerList:
	name = nodesById.get(id)
	props = allPropsById[id]
	cmdbId = props.get(cmdbIdName, '')
	criticality = props.get(criticalityName, '')
	status = props.get(statusName, '')
	print >>fcont, "%s,%s,%s,%s,,,,,,,,,,,,,,,,,,,%s,,,,,,,,,,,,,,,,,,,,,,%s" \
							% (cmdbId, company, name, criticality, nodeDescByName[name], status)
fcont.close

fdb = open("new-cmdb-san.csv", "w")
fdbtemplate = open("SAN_Import_Template.csv")
for t in fdbtemplate:
	print >>fdb,t
fdbtemplate.close
for sanId in newSanList:
	name = nodesById.get(sanId)
	props = allPropsById[sanId]
	cls = props.get(classPropName, '')
	cmdbId = props.get(cmdbIdName, '')
	model = props.get(modelName, '')
	if model == "Unknown": model = ''
	manufacturer = props.get(manuName, '')
	if manufacturer == "Unknown": manufacturer = ''
	fn = props.get(fnName, '')
	if fn == "Unknown": fn = ''
	location = props.get(locationName, '')
	if location == "Unknown": location = ''
	ipAddress = props.get(ipName, '')
	if ipAddress == "Unknown": ipAddress = ''
	status = props.get(statusName, '')
	devType = props.get(deviceTypeName, '')
	if devType == "Unknown": devType = ''
	serial = props.get(serialName, '')
	if serial == "Unknown": serial = ''
	print >>fdb, "%s,%s,%s,,%s,,,,,,,%s,,,,,,,,,,,,,,,,,,,,,,%s,%s,,,,,,,%s,,,,,,,,,,,,%s,,,,,,,,,,,,,,,,%s,,%s" \
							% (cmdbId,name,company,model, location, nodeDescByName[name], devType, fn, manufacturer, serial, status)

fdb.close

fdb = open("new-cmdb-sanfabric.csv", "w")
fdbtemplate = open("SAN_Fabric_Import_Template.csv")
for t in fdbtemplate:
	print >>fdb,t
fdbtemplate.close
for sanId in newSanFabricList:
	name = nodesById.get(sanId)
	props = allPropsById[sanId]
	cls = props.get(classPropName, '')
	cmdbId = props.get(cmdbIdName, '')
	model = props.get(modelName, '')
	if model == "Unknown": model = ''
	manufacturer = props.get(manuName, '')
	if manufacturer == "Unknown": manufacturer = ''
	fn = props.get(fnName, '')
	if fn == "Unknown": fn = ''
	location = props.get(locationName, '')
	if location == "Unknown": location = ''
	ipAddress = props.get(ipName, '')
	if ipAddress == "Unknown": ipAddress = ''
	status = props.get(statusName, '')
	devType = props.get(deviceTypeName, '')
	if devType == "Unknown": devType = ''
	serial = props.get(serialName, '')
	if serial == "Unknown": serial = ''
	print >>fdb, "%s,%s,%s,,,,,,,,,,,,,,,,,,,,,%s,%s,,,,,,%s,,,,,,,,,,,,,%s,,%s,%s,,,%s,,,,,,,,,,,,,,,%s" \
							% (cmdbId,name,company,nodeDescByName[name],devType,fn,location, manufacturer, model, status, serial, )

fdb.close

