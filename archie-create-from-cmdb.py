#Snow CMDB synchronisation with Archie - Archie import
#Compares CMDB export with Archie export and creates Archie import entries
#Author: Danny Andersen

import sys
import uuid

def inSubnet(subnet, mask, ipToCheck):
	if (subnet == '' or mask == '' or ipToCheck == ''):
		print "Empty field, cant look for subnet for IP: %s, %s, %s" % (subnet,mask,ipToCheck)
		return False
	subs = subnet.split('.')
	mk = mask.split('.')
	ip = ipToCheck.split('.')
	return (int(ip[0]) & int(mk[0]) == int(subs[0])) and (int(ip[1]) & int(mk[1]) == int(subs[1])) and int(ip[2]) & int(mk[2]) == int(subs[2]) and int(ip[3]) & int(mk[3]) == int(subs[3])

def findSubnet(ipAddr):
	ret = ("0", "0", "0")
	for id, subnet, mask in subnets:
		if inSubnet(subnet, mask, ipAddr): 
			ret = (id, subnet, mask)
			break;
	return ret
	
rels=list()
netrels=list() #List of tuples (parent, type, child, name)
existingRels = dict() #Key = (parent, type, child), val = rel id
existingProps = dict() #Keyed by node id + property name
nodes = dict() #id keyed by name
nodesFirstName = dict() #id keyed by nodes first word
hosts = dict()
servers = dict()
apps = dict()
devs = dict()
lpars = dict()
nets = dict()
buss = dict()
subnets = list()
props = dict()

cmdbIdName = "CMDB ID"
className = "CMDB Class"
deviceTypeName = "CMDB Device Type"
osName = "CMDB Operating System"
fnName = "CMDB Function"
ipName = "CMDB IP Address"
manuName = "CMDB Manufacturer"
modelName = "CMDB Model"
locationName = "CMDB Location"
installName = "CMDB Installation Path"
serialName = "CMDB Serial"
statusName = "CMDB Status"

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

#Read in existing nodes from exported file
fnodes = open("elements.csv", "r")
count = 0
for line in fnodes:
	count += 1
	if count == 1: continue
	lstr = line.rstrip('\n\r')
	fs = lstr.split(",")
	if len(fs) < 3 or not (fs[0].startswith('"')) : continue
	#print fs[0], fs[1], fs[2]
	id = fs[0].strip('"')
	nodeName = fs[2].strip('"').lower()
	nodeType = fs[1].strip('"')
	nodes[nodeName] = id
	if nodeType == "Node" and "(" in nodeName: 
		name = nodeName.split(" ")[0]
		nodesFirstName[name] = id
	if nodeType == "Node" and "." in nodeName: 
		name = nodeName.split(".")[0]
		#print "Name: %s: First name: %s\n" % (nodeName, name)
		nodesFirstName[name] = id
fnodes.close
#print "Existing nodes: " + str(count) + ": " + str(len(nodes))

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
		#relKey = fs[4].strip('"') + "-" + fs[1].strip('"') + "-" + fs[5].strip('"')
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
		id = fs[0]
		name = fs[1]
		val = fs[2]
		propKey = (id.strip('"'), name.strip('"'))
		existingProps[propKey] = val.strip('"')
		lstr = ""
fprops.close

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
		if (nodes[baseAddr], className) not in existingProps:
			props[(nodes[baseAddr], className)] = "cmdb_ci_subnet"
		continue
	docStr = desc
	if site != '':
		docStr += "\nSite: %s" % site
	id = str(uuid.uuid4())
	nets[baseAddr] = (id, docStr)
	subnets.append((id, baseAddr, mask))
	props[(id, className)] = "cmdb_ci_subnet"
	#print "subnet %s: %s, %s, %s" % (subnet, id, baseAddr, mask)
fh.close

#Process CMDB export
fcmdb = open("SNOW CMDB.csv")
count = 0
for line in fcmdb:
	count += 1
	if count == 1: continue
	fields=line.rstrip("\n\r").split(",")
	cmdbId = fields[0].strip()
	name = fields[1].strip().split('#')[0]
	lowerName = name.lower()
	if ".nie.co.uk" in lowerName: 
		firstName = lowerName.split(".")[0]
	else:
		firstName = lowerName
	classField = fields[2].strip()
	status = fields[5]
	deviceType = fields[19]
	funType = fields[23]
	ipAddr = fields[24].strip()
	subnet = ('0', '0', '0')
	if ipAddr != '' : subnet = findSubnet(ipAddr)
	location = fields[33]
	manufacturer = fields[34].strip("(Manufacturer)")
	model = fields[35]
	serial = fields[42]
	#skip dev/test / retired / absent / EUC
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
	if classField == computerClass or classField == printerClass : continue #Ignore all EUC devices
	#if classField == appClass: continue #lots of system sofware in the CMDB, not just apps

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
		print "WARNING: CMDB name %s: Unrecognised CMDB class field: %s - ignoring entry" % (name, classField)
		continue
	if lowerName in nodes or lowerName in nodesFirstName or firstName in nodes: 
		#Check all properties are set
		if lowerName in nodes: nodeId = nodes[lowerName]
		elif lowerName in nodesFirstName: nodeId = nodesFirstName[lowerName]
		else: nodeId = nodes[firstName]
		props[(nodeId, cmdbIdName)] = cmdbId #Always override id
		if (nodeId, className) not in existingProps:
			props[(nodeId, className)] = cmdbClass
		if location != '' and (nodeId, locationName) not in existingProps:
			props[(nodeId, locationName)] = location
		if deviceType != '' and (nodeId, deviceTypeName) not in existingProps:
			props[(nodeId, deviceTypeName)] = deviceType
		if funType != '' and (nodeId, fnName) not in existingProps:
			props[(nodeId, fnName)] = funType
		if ipAddr != '':
			if (nodeId, ipName) not in existingProps:
				props[(nodeId, ipName)] = ipAddr
			rel = (nodeId, "AssociationRelationship", subnet[0])
			if subnet[0] != "0" and rel not in existingRels: 
				netrels.append((nodeId, "AssociationRelationship", subnet[0], ipAddr))
		if manufacturer != '' and (nodeId, manuName) not in existingProps:
			props[(nodeId, manuName)] = manufacturer
		if model != '' and (nodeId, modelName) not in existingProps:
			props[(nodeId, modelName)] = model
		if serial != '' and (nodeId, serialName) not in existingProps:
			props[(nodeId, serialName)] = serial
		if status != '' and (nodeId, statusName) not in existingProps:
			props[(nodeId, statusName)] = status
	else :
		if name.startswith("NIE-CTX") : continue #Cytrix nodes are wrong currently
		if status == "Retired" or status == "Absent" : continue
		if classField == appClass:
			print "WARNING: Ignoring application %s" % name
			continue #lots of system sofware in the CMDB, not just apps
		#if (classField == serverClass or classField == aixServerClass) and "(" in name: continue #ignore servers with ( in name
		#Create node
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
							or classField == winClass or classField == linuxClass:
			servers[name] = (nodeId, dstr.strip('"'))
		elif classField == dbClass: apps[name] = (nodeId, dstr.strip('"'))
		elif classField == netClass or classField == storageClass: devs[name] = (nodeId, dstr.strip('"'))
		else : 
			print "WARNING: CMDB name %s: Unrecognised CMDB class field: %s - cannot add new node" % (name, classField)
			continue				
		props[(nodeId, className)] = cmdbClass
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

fcmdb.close
	

felems = open("new-elements.csv", "w")
print >>felems,'"ID","Type","Name","Documentation"'
for h in hosts:
	print >>felems,'"%s","Node","%s","%s"' % (hosts[h][0],h,hosts[h][1])
for s in servers:
	print >>felems,'"%s","Node","%s","%s"' % (servers[s][0],s, servers[s][1])
for a in apps:
	print >>felems,'"%s","ApplicationComponent","%s","%s"' % (apps[a][0], a, apps[a][1])
for net in nets:
	print >>felems,'"%s","CommunicationNetwork","%s","%s"' % (nets[net][0], net, nets[net][1])
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
print >>fprops,'"ID","Key","Value"'
for prop in props:
	print >>fprops, '"%s","%s","%s"' % (prop[0], prop[1], props[prop])
fprops.close	
