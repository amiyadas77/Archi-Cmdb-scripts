#Compares Vmware RVTools export files with Archie export and creates new Archie import for elements / properties / rels 
#Author: Danny Andersen

import sys
import os
import uuid

import xlrd
from cmdbconstants import *

rels=list()
netrels=list() #List of tuples (parent, type, child, name)
existingRels = dict() #Key = (parent, type, child), val = rel id
existingProps = dict() #Keyed by node id + property name
allPropsById = dict() #dict of dict of all properties found for a particular element keyed by its id  and the prop name
nodesFirstName = dict() #id keyed by nodes first word
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

sysSoftware = dict() #id keyed by systemsoftware
hosts = dict()
servers = dict()
apps = dict()
devs = dict()
lpars = dict()
nets = dict()
buss = dict()
props = dict()
softs = dict() #New syssoft to add

vm = "VM"
powerState = "Powerstate"
dnsStr = "DNS Name"
cpuStr = "CPUs"
memoryStr = "Memory"
ipStr = "IP Address"
osStr = "OS"
hostStr = "Host"
clStr = "Cluster"
esxStr = "ESX Version"
domainStr = "Domain"
cpuModelStr = "CPU Model"
modelStr = "Model"
noCPUStr = "# CPU"
noCoresStr = "# Cores"
noMemory = "# Memory"

rvToolsLookup = {cpuStr: cpuName, memoryStr: memName, \
					osStr: osName, ipStr: ipName }
					
#Read in existing nodes from exported file
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
	if nodeType == "SystemSoftware":
		sysSoftware[name.lower()] = id
	
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

#Load in subnets - can only be done once elements have been read
loadSubnets()

def processVHost(cols, row):
	host = row[cols[hostStr]].value.strip().split('.')[0]
	if host.lower() == "nie-omat-01" or host.lower() == "nie-omat-02":
		host += " (PHY)"
	domain = row[cols[domainStr]].value.strip()
	esx = row[cols[esxStr]].value.strip()
	cluster = row[cols[clStr]].value.strip()
	serverModel = row[cols[modelStr]].value.strip()
	cpuModel = row[cols[cpuModelStr]].value.strip()
	cpus = "%d" % row[cols[noCPUStr]].value
	cores = "%d" % row[cols[noCoresStr]].value
	ram = "%d GB" % (row[cols[noMemory]].value/1024)

	lowerHost = host.lower()
	if lowerHost in nodesByName:
		hostId = nodesByName[lowerHost]
		if (hostId, classPropStr) not in existingProps:
			props[(hostId, classPropStr)] = "cmdb_ci_esx_server"
		if (hostId, deviceTypeName) not in existingProps:
			props[(hostId, deviceTypeName)] = "Physical Server"
		if (hostId, osName) not in existingProps:
			props[(hostId, osName)] = esx
		if (hostId, fnName) not in existingProps:
			props[(hostId, fnName)] = "ESX Server"
		if (hostId, domainName) not in existingProps and domain != '':
			props[(hostId, domainName)] = domain
	else:
		print "Found new vm host: " + host
		docStr = "VM Host Server\nServer Model: " + serverModel
		docStr += "\nOperating System: " + esx 
		docStr += "\nProcessor: " + cpuModel
		docStr += "\n%s CPU %s Cores %s RAM" % (cpus, cores, ram)
		docStr += "\nVCenter cluster: " + cluster
		id = str(uuid.uuid4())
		hosts[host] = (id, docStr)
		nodesById[id] = host
		props[(id, classPropStr)] = "cmdb_ci_esx_server"
		props[(id, deviceTypeName)] = "Physical Server"
		props[(id, osName)] = esx
		if domain != '': props[(id, domainName)] = domain

def processVNetwork(cols, row):
	#Find subnet
	server = row[cols[vm]].value.strip()
	ipAddr = row[cols[ipStr]].value.strip()
	#print server, ipAddr
	if ipAddr != "unknown":
		if ',' in ipAddr:
			#Remove ipv6 part
			fs = ipAddr.split(',')
			for f in fs:
				if '.' in f:
					ipAddr = f.strip()
					break
		subnet = ('0', '0', '0')
		if ipAddr != '' : subnet = findSubnet(ipAddr)
		if server.lower() in nodesByName:
			id = nodesByName[server.lower()]
		else:
			(id, docStr) = servers[server]
			#New server - add in IP addr
			docStr += "\nIP Address: " + ipAddr
			servers[server] = (id, docStr)
		if ipAddr != '' and (id, ipName) not in existingProps:
			props[(id, ipName)] = ipAddr
		rel = (id, "AssociationRelationship", subnet[0])
		if subnet[0] != "0" and rel not in existingRels: 
			netrels.append((id, "AssociationRelationship", subnet[0], ipAddr))

#Process row in spreadsheet
def processVInfo(cols, row):
	server = row[cols[vm]].value.strip()
	osystem = row[cols[osStr]].value.strip()
	host = row[cols[hostStr]].value.strip()
	powered = row[cols[powerState]].value.strip()
	fqdn = row[cols[dnsStr]].value.strip()
	cluster = row[cols[clStr]].value.strip()
	cpus = "%d" % row[cols[cpuStr]].value
	ram = "%d GB" % (row[cols[memoryStr]].value/1024)

	#print server, osystem
	if fqdn == server: domain = "nie.co.uk" #Default to default domain
	else:
		parts = fqdn.split('.')
		domain = ''
		for d in range(1,len(parts)):
			domain += parts[d] + '.'
		domain = domain.rstrip('.')
	#Skip VMs powered off
	if powered != "poweredOff":
		hostId = ""
		if host in hosts :
			hostId = hosts[host][0]
		elif host in nodesByName :
			hostId = nodesByName[host]
		if server.lower() in nodesByName:
			server = server.lower()
			id = nodesByName[server]
			#Check relationships are set
			if hostId != "":
				rel = (hostId, "CompositionRelationship", nodesByName[server])
				if not (rel in existingRels): rels.append(rel)
			if osystem.lower() not in sysSoftware:
				#Add OS to system software
				osid = str(uuid.uuid4())
				softs[osystem] = (osid, osystem) #Add to ones to create
				sysSoftware[osystem.lower()] = osid #Add to existing node set
				rel = (id, "AssignmentRelationship", osid)
				if rel not in existingRels : rels.append(rel)
			else:
				rel = (id, "AssignmentRelationship", sysSoftware[osystem.lower()])
				if rel not in existingRels : rels.append(rel)
			if (id, classPropStr) not in existingProps:
				if "windows" in osystem.lower():
					props[(id, classPropStr)] = "cmdb_ci_win_server"
				elif "linux" in osystem.lower():
					props[(id, classPropStr)] = "cmdb_ci_linux_server"
				elif "solaris" in osystem.lower():
					props[(id, classPropStr)] = "cmdb_ci_solaris_server"
				elif "aix" in osystem.lower():
					props[(id, classPropStr)] = "cmdb_ci_aix_server"
				else:
					props[(id, classPropStr)] = "cmdb_ci_server"
			if (id, deviceTypeName) not in existingProps:
				props[(id, deviceTypeName)] = "Virtual Server"
			if (id, osName) not in existingProps:
				props[(id, osName)] = osystem
			if (id, domainName) not in existingProps and domain != '':
				props[(id, domainName)] = domain
		else :
			docStr = "Vmware VM\nOperating System: " + osystem
			docStr += "\n%s vCPU %s RAM" % (cpus, ram)
			docStr += "\nCluster " + cluster
			id = str(uuid.uuid4())
			servers[server] = (id, docStr)
			nodesById[id] = server
			if hostId != "" :
				rels.append((hostId, "CompositionRelationship", id))
			if "windows" in osystem.lower():
				props[(id, classPropStr)] = "cmdb_ci_win_server"
			elif "linux" in osystem.lower():
				props[(id, classPropStr)] = "cmdb_ci_linux_server"
			elif "solaris" in osystem.lower():
				props[(id, classPropStr)] = "cmdb_ci_solaris_server"
			elif "aix" in osystem.lower():
				props[(id, classPropStr)] = "cmdb_ci_aix_server"
			else:
				props[(id, classPropStr)] = "cmdb_ci_server"
			props[(id, deviceTypeName)] = "Virtual Server"			
			props[(id, osName)] = osystem
			if osystem.lower() not in sysSoftware:
				#Add OS to system software
				osid = str(uuid.uuid4())
				softs[osystem] = (osid, osystem) #Add to ones to create
				sysSoftware[osystem.lower()] = osid #Add to existing node set
				rel = (id, "AssignmentRelationship", osid)
				rels.append(rel)
			else:
				rel = (id, "AssignmentRelationship", sysSoftware[osystem.lower()])
				rels.append(rel)
			if domain != '':
				props[(id, domainName)] = domain

def rowToCsv(row):
	out = ""
	for c in row:
		out += str(c.value)
		out += ","
	return out

#Process each RVTools workbook
for file in os.listdir('.'):
	if file.startswith("RVTools_") and file.endswith(".xls"):
		wb = xlrd.open_workbook(file)
		ws = wb.sheet_by_name('tabvHost')
		count = 0
		for row in ws.get_rows():
			#print rowCsv
			count += 1
			if count == 1:
				rowCsv = rowToCsv(row)
				cols = processHeader(rowCsv)
			else:
				processVHost(cols, row)
		ws = wb.sheet_by_name('tabvInfo')
		count = 0
		for row in ws.get_rows():
			#print rowCsv
			count += 1
			if count == 1:
				rowCsv = rowToCsv(row)
				cols = processHeader(rowCsv)
			else:
				processVInfo(cols, row)
		ws = wb.sheet_by_name('tabvNetwork')
		count = 0
		for row in ws.get_rows():
			#print rowCsv
			count += 1
			if count == 1:
				rowCsv = rowToCsv(row)
				cols = processHeader(rowCsv)
			else:
				#Add to list of VMs 
				processVNetwork(cols, row)

exit

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
fread = open("new-properties-readable.csv", "w")
print >>fprops,'"ID","Key","Value"'
print >>fread,'"Name","Key","Value"'
for prop in props:
	print >>fprops, '"%s","%s","%s"' % (prop[0], prop[1], props[prop])
	print >>fread, '"%s","%s","%s"' % (nodesById[prop[0]], prop[1], props[prop])
fprops.close
fread.close

