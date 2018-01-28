#Compares Certero scan export files with Archie export and creates new Archie import for elements / properties / rels 
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
sysSoftware = dict() #id keyed by systemsoftware
hosts = dict()
servers = dict()
apps = dict()
devs = dict()
lpars = dict()
nets = dict()
buss = dict()
subnets = list()
props = dict()
softs = dict() #New syssoft to add

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
domainName = "CMDB Domain DNS"

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
	if nodeType == "SystemSoftware":
		sysSoftware[nodeName.lower()] = id
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

fh = open("IBMphysical.csv", "r")
count = 0
for line in fh:
	count += 1
	if count == 1: continue
	lstr = line.rstrip('\n\r')
	fs = lstr.split(",")
	host = fs[0]
	#skip dev/test
	#if host.startswith("nied") : continue
	#Skip if already set in elements file
	if host.lower() in nodes :
		#print "host " + host + " already exists"
		#Check Class exists
		host = host.lower()
		if (nodes[host], className) not in existingProps:
			props[(nodes[host], className)] = "cmdb_ci_aix_server"
		if (nodes[host], deviceTypeName) not in existingProps:
			props[(nodes[host], deviceTypeName)] = "Physical Server"
	else:
		docStr = "LPAR Host Server\nServer Model: p750 " + fs[1]
		docStr += "\nSerial Number: " + fs[2]
		docStr += "\nProcessors: %s (%s installed)" % (fs[6],fs[5])
		docStr += "\nMemory : " + fs[12] + "( " + fs[11] + " installed), Available: " + fs[14]
		id = str(uuid.uuid4())
		hosts[host] = (id, docStr)
		props[(id, className)] = "cmdb_ci_aix_server"
		props[(id, deviceTypeName)] = "Physical Server"
fh.close


fh = open("vlans.csv", "r")
count = 0
for line in fh:
	count += 1
	if count == 1: continue
	lstr = line.rstrip('\n\r')
	fs = lstr.split(",")
	vlan = "VLAN " + fs[1]
	baseAddr = fs[3].split('/')[0]
	mask = fs[4]
	if fs[1] == "":
		vlan = fs[3]
	#Skip if already set in elements file
	if vlan == "": continue
	if vlan.lower() in nodes :
		#print "vlan %s already exists" % vlan
		vlan = vlan.lower()
		subnets.append((nodes[vlan], baseAddr, mask))
		if (nodes[vlan], className) not in existingProps:
			props[(nodes[vlan], className)] = "cmdb_ci_subnet"
		continue
	docStr = "VLAN Name: " + fs[0]
	docStr += "\nVLAN Description: " + fs[2]
	docStr += "\nVLAN IP Address/Bits: " + fs[3]
	docStr += "\nSubnet Mask: " + mask
	docStr += "\nGateway: " + fs[5]
	id = str(uuid.uuid4())
	nets[vlan] = (id, docStr)
	subnets.append((id, baseAddr, mask))
	props[(id, className)] = "cmdb_ci_subnet"
	#print "vlan %s: %s, %s, %s" % (vlan, id, baseAddr, mask)
fh.close

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

fh = open("vm-hosts.csv", "r")
count = 0
for line in fh:
	count += 1
	if count == 1: continue
	lstr = line.rstrip('\n\r')
	fs = lstr.split(",")
	host = fs[0]
	domain = fs[1]
	dc = fs[21]					
	os = fs[2]
	#Skip Dev/test hosts
	#if dc == "ESD-NIE" or dc == "NIE-DRG" : continue
	#Skip if already set in nodes file
	lowerHost = host.lower()
	if lowerHost in nodes:
		hostId = nodes[lowerHost]
		if (hostId, className) not in existingProps:
			props[(hostId, className)] = "cmdb_ci_esx_server"
		if (hostId, deviceTypeName) not in existingProps:
			props[(hostId, deviceTypeName)] = "Physical Server"
		if (hostId, osName) not in existingProps:
			props[(hostId, osName)] = os
		if (hostId, fnName) not in existingProps:
			props[(hostId, fnName)] = "ESX Server"
		if (hostId, domainName) not in existingProps and domain != '':
			props[(hostId, domainName)] = domain
	else:
		#print "new vm host: " + host
		docStr = "VM Host Server\nServer Model: " + fs[8]
		docStr += "\nOperating System: " + os + " Version: " + fs[3]
		docStr += "\nProcessor: " + fs[10]
		docStr += "\n" + fs[11] + " CPU " + fs[12] + " Cores " + fs[14] + " RAM"
		docStr += "\nVCenter cluster: " + fs[16]
		id = str(uuid.uuid4())
		hosts[host] = (id, docStr)
		props[(id, className)] = "cmdb_ci_esx_server"
		props[(id, deviceTypeName)] = "Physical Server"
		props[(id, osName)] = os
		if domain != '': props[(id, domainName)] = domain
fh.close

# fh = open("missing-ip-addresses.csv", "r")
# count = 0
# for line in fh:
	# count += 1
	# if count == 1: continue
	# serverStr = line.rstrip('\n\r')
	# fs = serverStr.split(",")
	# server = fs[0]
	# ipAddr = fs[1]
	# desc = fs[2]
	# #Skip Dev/test hosts
	# if server.startswith("billing2"): continue
	# #Find subnet
	# subnet = ('0', '0', '0')
	# if ipAddr != '' : subnet = findSubnet(ipAddr)
	# if server in nodes or server in nodesFirstName:
		# #Only add missing IPs to existing nodes
		# #Check relationships are set
		# if server in nodes: servId = nodes[server]
		# else: servId = nodesFirstName[server]
		# rel = (servId, "AssociationRelationship", subnet[0])
		# if subnet[0] != "0" and rel not in existingRels: 
			# netrels.append((servId, "AssociationRelationship", subnet[0], ipAddr))
		# sClass = ""
		# if server.startswith("billing"): 
			# sClass = "cmdb_ci_aix_server"
		# else:
			# sClass = "cmdb_ci_vmware_instance"
		# if (servId, className) not in existingProps:
			# props[(servId, className)] = sClass
		# if (servId, deviceTypeName) not in existingProps:
			# props[(servId, deviceTypeName)] = "Virtual Server"
		# if ipAddr != '' and (servId, ipName) not in existingProps:
			# props[(servId, ipName)] = ipAddr
	# else:
		# docStr = desc
		# docStr += "\nIP Address: " + ipAddr
		# id = str(uuid.uuid4())
		# servers[server] = (id, docStr)
		# #print server + " host " + fs[7] + " IP " + fs[4]
		# if subnet[0] != "0" :
			# netrels.append((id, "AssociationRelationship", subnet[0], ipAddr))
		# props[(id, className)] = "cmdb_ci_vmware_instance"
		# props[(id, deviceTypeName)] = "Virtual Server"			
		# props[(id, ipName)] = ipAddr
fh.close

fh = open("physical-servers.csv", "r")
count = 0
for line in fh:
	count += 1
	if count == 1: continue
	lstr = line.rstrip('\n\r')
	fs = lstr.split(",")
	host = fs[0]
	location = fs[2]
	ipAddr = fs[1]
	os = fs[12]
	manu = fs[7]
	model = fs[8]
	domain = fs[10]
	if domain != '':
		if domain == "NIE" : domain = "nie.co.uk"
		elif domain == "ES-NIE": domain = "es-nie.local"
		elif domain == "NMS-NIE": domain = "nms-nie.local"
	#Find subnet
	subnet = ('0', '0', '0')
	if ipAddr != '' : subnet = findSubnet(ipAddr)
	#Skip Dev/test hosts
	#if host.startswith("ESD-NIE") or host.startswith("NIE-DG") : continue
	#if "Dargan" in location: continue
	#Skip if already set in nodes file
	if host.lower() in nodes:
		hostId = nodes[host.lower()]
		if os.lower() not in sysSoftware:
			#Add OS to system software
			osid = str(uuid.uuid4())
			softs[os] = (osid, os) #Add to ones to create
			sysSoftware[os.lower()] = osid #Add to existing node set
			rel = (id, "AggregationRelationship", osid)
			rels.append(rel)
		else:
			rel = (hostId, "AggregationRelationship", sysSoftware[os.lower()])
			if rel not in existingRels : rels.append(rel)
		#if (hostId, className) not in existingProps:
		if "windows" in os.lower():
			props[(hostId, className)] = "cmdb_ci_win_server"
		elif "linux" in os.lower():
			props[(hostId, className)] = "cmdb_ci_linux_server"
		elif "solaris" in os.lower():
			props[(hostId, className)] = "cmdb_ci_solaris_server"
		elif "aix" in os.lower():
			props[(hostId, className)] = "cmdb_ci_aix_server"
		else:
			props[(hostId, className)] = "cmdb_ci_server"
		if (hostId, deviceTypeName) not in existingProps:
			props[(hostId, deviceTypeName)] = "Physical Server"
		if (hostId, osName) not in existingProps:
			props[(hostId, osName)] = os
		if manu != '' and (hostId, manuName) not in existingProps:
			props[(hostId, manuName)] = manu
		if  model != '' and (hostId, modelName) not in existingProps:
			props[(hostId, modelName)] = model
		if  location != '' and (hostId, locationName) not in existingProps:
			props[(hostId, locationName)] = location
		if ipAddr != '' and (hostId, ipName) not in existingProps:
			props[(hostId, ipName)] = ipAddr
		if (hostId, domainName) not in existingProps and domain != '':
			props[(hostId, domainName)] = domain
		rel = (hostId, "AssociationRelationship", subnet[0])
		if subnet[0] != "0" and rel not in existingRels: 
			netrels.append((hostId, "AssociationRelationship", subnet[0], ipAddr))
	else:
		#print "new host: " + host
		docStr = "Physical Server\nManufacturer: %s\nModel: %s" % (manu, model)
		docStr += "\nOperating System: " + os + " Version: " + fs[13]
		docStr += "\nProcessor: " + fs[15]
		docStr += "\n" + fs[17] + " CPU " + fs[18] + " Cores "
		docStr += "\nIP Address: " + ipAddr
		docStr += "\nLocation: " + location
		if subnet[0] != "0" :
			netrels.append((id, "AssociationRelationship", subnet[0], ipAddr))
		id = str(uuid.uuid4())
		hosts[host] = (id, docStr)
		props[(id, className)] = "cmdb_ci_server"
		props[(id, deviceTypeName)] = "Physical Server"
		props[(id, manuName)] = manu
		props[(id, modelName)] = model
		props[(id, locationName)] = location
		props[(id, ipName)] = ipAddr
		if domain != '': props[(hostId, domainName)] = domain
		if os.lower() not in sysSoftware:
			#Add OS to system software
			osid = str(uuid.uuid4())
			softs[os] = (osid, os)
			sysSoftware[os.lower()] = osid #Add to existing node set
			rel = (id, "AggregationRelationship", osid)
			rels.append(rel)
		else:
			rel = (id, "AggregationRelationship", sysSoftware[os.lower()])
			rels.append(rel)
fh.close

fin = open("virtual-machines.csv")
count = 0
for line in fin:
	count += 1
	if count == 1: continue
	serverStr = line.rstrip('\n\r')
	fs = serverStr.split(",")
	server = fs[0]
	os = fs[1]
	host = fs[7]
	dc = fs[12]
	ipAddr = fs[4]
	fqdn = fs[3]
	if fqdn == server: domain = "nie.co.uk" #Default to default domain
	else:
		parts = fqdn.split('.')
		domain = ''
		for d in range(1,len(parts)):
			domain += parts[d] + '.'
		domain = domain.rstrip('.')
	#Skip Dev/test hosts
	#if dc == "ESD-NIE" or dc == "NIE-DRG" : continue
	#Skip VMs powered off
	if fs[2] == "Powered Off" : continue
	#Find subnet
	subnet = ('0', '0', '0')
	if ipAddr != '' : subnet = findSubnet(ipAddr)
	servId = ""
	if host in hosts :
		servId = hosts[host][0]
	elif host in nodes :
		servId = nodes[host]
	#Skip if already set in nodes file
	if server.lower() in nodes:
		server = server.lower()
		id = nodes[server]
		#Check relationships are set
		if servId != "":
			rel = (servId, "CompositionRelationship", nodes[server])
			if not (rel in existingRels): rels.append(rel)
		if os.lower() not in sysSoftware:
			#Add OS to system software
			osid = str(uuid.uuid4())
			softs[os] = (osid, os) #Add to ones to create
			sysSoftware[os.lower()] = osid #Add to existing node set
			rel = (id, "AggregationRelationship", osid)
			if rel not in existingRels : rels.append(rel)
		else:
			rel = (id, "AggregationRelationship", sysSoftware[os.lower()])
			if rel not in existingRels : rels.append(rel)
		rel = (id, "AssociationRelationship", subnet[0])
		if subnet[0] != "0" and rel not in existingRels: 
			netrels.append((id, "AssociationRelationship", subnet[0], ipAddr))
		#if (id, className) not in existingProps:
		if "windows" in os.lower():
			props[(id, className)] = "cmdb_ci_win_server"
		elif "linux" in os.lower():
			props[(id, className)] = "cmdb_ci_linux_server"
		elif "solaris" in os.lower():
			props[(id, className)] = "cmdb_ci_solaris_server"
		elif "aix" in os.lower():
			props[(id, className)] = "cmdb_ci_aix_server"
		else:
			props[(id, className)] = "cmdb_ci_server"
		if (id, deviceTypeName) not in existingProps:
			props[(id, deviceTypeName)] = "Virtual Server"
		if ipAddr != '' and (id, ipName) not in existingProps:
			props[(id, ipName)] = ipAddr
		if (id, osName) not in existingProps:
			props[(id, osName)] = os
		if (id, domainName) not in existingProps and domain != '':
			props[(id, domainName)] = domain
	else :
		docStr = "Vmware VM\nOperating System: " + os
		docStr += "\nIP Address: " + ipAddr
		docStr += "\n" + fs[5] + " CPU " + fs[6] + " RAM"
		docStr += "\nCluster " + fs[8]
		id = str(uuid.uuid4())
		servers[server] = (id, docStr)
		#print server + " host " + fs[7] + " IP " + fs[4]
		if servId != "" :
			rels.append((servId, "CompositionRelationship", id))
		if subnet[0] != "0" :
			netrels.append((id, "AssociationRelationship", subnet[0], ipAddr))
		props[(id, className)] = "cmdb_ci_vmware_instance"
		props[(id, deviceTypeName)] = "Virtual Server"			
		props[(id, ipName)] = ipAddr
		props[(id, osName)] = os
		if os.lower() not in sysSoftware:
			#Add OS to system software
			osid = str(uuid.uuid4())
			softs[os] = (osid, os) #Add to ones to create
			sysSoftware[os.lower()] = osid #Add to existing node set
			rel = (id, "AggregationRelationship", osid)
			rels.append(rel)
		else:
			rel = (id, "AggregationRelationship", sysSoftware[os.lower()])
			rels.append(rel)
		if domain != '':
			props[(id, domainName)] = domain
fin.close

fin = open("lpars.csv")
count = 0
for line in fin:
	count += 1
	if count == 1: continue
	serverStr = line.rstrip('\n\r')
	fs = serverStr.split(",")
	host = fs[1]
	lparName = fs[0]
	os = fs[5]
#	lpar = fs[0] + " (" + host + ")"
	lpar = fs[0]
	if lpar.startswith("vio") and not lpar.startswith("vios_genapps"): lpar = fs[0] + " (" + host + ")"
#	if lpar in servers:
#		#Same lpar name exists - add in physical name
#		dupe = servers[lpar]
#		#print lpar, dupe, type(dupe)
#		del servers[lpar]
#		lpar += "-" + host
#		servers[lpar] = dupe
	servId = ""
	#skip dev/test
	#if host.startswith("nied") : continue
	#if lparName.startswith("nms") and ("dr" in lparName or "tst" in lparName or "pre" in lparName): continue
	if host in hosts:
		servId = hosts[host][0]
	elif host in nodes:
		servId = nodes[host]
	if lpar.lower() in nodes:
		lpar = lpar.lower()
		#LPAR already exists - check relationship is set
		if servId != "":
			rel = (servId, "CompositionRelationship", nodes[lpar])
			if not (rel in existingRels): rels.append(rel)
		id = nodes[lpar]
		#Add to dict of lpar ids by name
		lpars[lparName] = id 
		if (id, className) not in existingProps:
			props[(id, className)] = "cmdb_ci_mainframe_lpar"
		if (id, deviceTypeName) not in existingProps:
			props[(id, deviceTypeName)] = "Virtual Server"
		if (id, osName) not in existingProps:
			props[(id, osName)] = os
	else :
		#Add new node
		docStr = "LPAR" + "\nOperating System: " + os
		docStr += "\nLpar ID: " + fs[2]
		docStr += "\n" + fs[10] + " CPUs " + fs[17] + " RAM"
		id = str(uuid.uuid4())
		servers[lpar] = (id, docStr)
		lpars[lparName] = id
		#print lpar + " host " + host 
		if servId != "" :
			rels.append((servId, "CompositionRelationship", id))
		props[(id, className)] = "cmdb_ci_mainframe_lpar"
		props[(id, deviceTypeName)] = "Virtual Server"
		props[(id, osName)] = os
fin.close
	
#Process SQL servers
fdb = open("SQL Instances.csv")
count = 0
for line in fdb:
	count += 1
	if count == 1: continue
	fields=line.split(",")
	dbname = fields[1] + "-" + fields[0]
	if fields[2] != "Database Engine" : dbname += " (" + fields[2] + ")"
	if dbname in apps:
		print "Warning: 2 SQL instances with the same name: " + dbname
		dbname += " (2)"
	serv = fields[0]
	srcid = ""
	if serv in nodes : 
		srcid = nodes[serv]
	elif serv in hosts:
		srcid = hosts[serv][0]
	elif serv in servers:
		srcid = servers[serv][0]
	#Skip Dev/Test DB
	#if "HVH" not in serv and (serv.startswith("ESD") or "DGN" in serv or "TEST" in serv) : continue
	#Skip DB if already exists
	if dbname.lower() in nodes :
		dbname = dbname.lower()
		if srcid != "": 
			#Check relationship is set
			rel = (srcid, "ServingRelationship", nodes[dbname])
			if not (rel in existingRels) : rels.append(rel)
		if (nodes[dbname], className) not in existingProps:
			props[(nodes[dbname], className)] = "cmdb_ci_db_mssql_instance"
	else :
		#Add DB as new application
		#print "New SQL DB: " + dbname + " srcid: " + srcid
		dstr = 'MS SQL Server Database %s %s\nVersion: %s\n' % (fields[3],fields[5],fields[6])
		id = str(uuid.uuid4())
		apps[dbname] = (id, dstr.strip('"'))
		if srcid != "" :
			rel =  (srcid, "ServingRelationship", id)
			rels.append(rel)
		props[(id, className)] = "cmdb_ci_db_mssql_instance"
fdb.close

#Process Oracle DBs
fdb = open("Oracle Homes.csv")
count = 0
for line in fdb:
	count += 1
	if count == 1: continue
	fields=line.rstrip("\n\r").split(",")
	dbHome = fields[1]
	dbname = dbHome + "-" + fields[0]
	if dbname in apps:
		print "Warning: 2 SQL instances with the same name: " + dbname
		dbname += " (2)"
	serv = fields[0]
	installPath = fields[2]
	#Find host
	srcid = ""
	if serv in nodes : 
		srcid = nodes[serv]
	elif serv in hosts:
		srcid = hosts[serv][0]
	elif serv in servers:
		srcid = servers[serv][0]
	elif serv in lpars:
		srcid = lpars[serv]
	#Skip Oracle client installs
	if "OraClient" in dbHome: continue
	if "client" in installPath: continue
	#Skip pure oracle agent installs
	if "agent" in installPath: continue
	#Skip java oracle agent installs
	if "jdk" in installPath: continue
	#Skip sysman  installs
	if "sysman" in installPath: continue
	#Skip Oracle middleware 
	if "middleware" in installPath: continue
	#Skip Dev/Test DB
	#if "HVH" not in serv and (serv.startswith("ESD") or "DGN" in serv or "TEST" in serv) : continue
	#if serv.startswith("nms") and ("dr" in serv or "tst" in serv or "pre" in serv): continue
	#if serv.startswith("billing2"): continue
	#Skip DB if already exists
	if dbname.lower() in nodes :
		dbname = dbname.lower()
		if srcid != "": 
			#Check relationship is set
			rel = (srcid, "ServingRelationship", nodes[dbname])
			if not (rel in existingRels) : rels.append(rel)
		if (nodes[dbname], className) not in existingProps:
			props[(nodes[dbname], className)] = "cmdb_ci_db_ora_instance"
		if (nodes[dbname], installName) not in existingProps:
			props[(nodes[dbname], installName)] = installPath
	else :
		#Add DB as new application
		#print "New SQL DB: " + dbname + " srcid: " + srcid
		dstr = 'Oracle Database\nHome: %s\nInstall Path: %s' % (dbHome,installPath)
		id = str(uuid.uuid4())
		apps[dbname] = (id, dstr.strip('"'))
		if srcid != "" :
			rel =  (srcid, "ServingRelationship", id)
			rels.append(rel)
		props[(id, className)] = "cmdb_ci_db_ora_instance"
		props[(id, installName)] = installPath
fdb.close

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
for s in softs:
	print >>felems,'"%s","SystemSoftware","%s","%s"' % (softs[s][0], s, softs[s][1])
	
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
