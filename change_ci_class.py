#Archie EA tool to Snow CMDB synchronisation script
#Compares CMDB export with Archie export and lists the CMDB Ci where the class needs to be changed
#Author: Danny Andersen

import sys
import uuid

#nodesByName = dict() #Keyed by node name, id of node
nodesById = dict() # Keyed by node id, name of node
cmdb = dict() #Keyed by Name, the cmdb class
cmdbId = dict() #Keyed by Name, the cmdb ID
nodeClass = dict() #Keyed by node id, the node class property
changeClassList = list() # list of element ids that have wrong class

cmdbIdName = "CMDB ID"
classPropName = "CMDB Class"

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
	if '#' in name: 
		#Names with this have been decommissioned, but not really in some cases
		#Only take the name as the bit before the #
		name = name.split('#')[0]
	if ".nie.co.uk" in name: 
		name = name.split(".")[0]
	id = fields[0]
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
	elif classField == vcenterClass: cmdbClass = vcenterStr
	elif classField == vmwareClass: cmdbClass = vmwareStr
	elif classField == vmClass: cmdbClass = vmStr
	elif classField == clusterClass: cmdbClass = clusterStr
	elif classField == rackClass: cmdbClass = rackStr
	elif classField == lbhwClass: cmdbClass = lbhwStr
	elif classField == lbswClass: cmdbClass = lbswStr
	else : 
		print "WARNING: CMDB name %s: Unrecognised CMDB class field: %s - ignoring entry" % (name, classField)
		continue
	cmdb[name] = cmdbClass
	cmdbId[name] = id
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
		if name == classPropName:
			nodeClass[id] = val
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
	fs = fullStr.rstrip('\n\r').split(",")
	fullStr = ''
	name = fs[2].strip('"')
	id = fs[0].strip('"')
	desc = fs[3]
	nodesById[id] = name
	if name.lower() in cmdb:
		if cmdb[name.lower()] != nodeClass[id]:
			#Add to list to change class
			changeClassList.append(id)
	elif nodeClass.get(id, '') != '':
		print "Warning: %s not in CMDB" % name.lower()
fnodes.close
#print "Existing nodes: " + str(count) + ": " + str(len(nodes))

fapp = open("change-cmdb-class.csv", "w")
print >>fapp, "CMDB Unique Id, Name, Old class, New class"
for id in changeClassList:
	name = nodesById.get(id)
	print >>fapp, "%s,%s,%s,%s" % (cmdbId[name.lower()], name, cmdb[name.lower()], nodeClass[id])
fapp.close

