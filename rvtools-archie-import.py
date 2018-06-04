#Compares Vmware RVTools export files with Archie export and creates new Archie import for elements / properties / rels 
#Author: Danny Andersen

#TODO 
#Add properties for CPU and Memory
#If OS is different - reset in both description and property
#Include NIE-TH-TLG hosts once Tooling cluster part of the RVtools


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
nameSwap = dict() # dict of replacement server names (DNS) keyed by server name
vmSetRaw = set() #Set of VM ids found in Archie before removing unwanted ones (LPARs, etc)
vmSet = set() # set of VM ids in Archie
vmProcessed = list() # list of Vm names that have been processed

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
osTools = "OS according to the VMware Tools"
hostStr = "Host"
clStr = "Cluster"
esxStr = "ESX Version"
domainStr = "Domain"
cpuModelStr = "CPU Model"
modelStr = "Model"
noCPUStr = "# CPU"
noCoresStr = "# Cores"
noMemory = "# Memory"

# rvToolsLookup = {cpuStr: cpuName, memoryStr: memName, \
					# osStr: osName, ipStr: ipName }
					
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
	nodeDescByName[lowerName] = desc
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
		#CMDB Model = Virtual Machine or CMDB Device Type = Virtual Server or CMDB Manufacturer = VMware)
		if (name == deviceTypeName and val == "Virtual Server"):
			vmSetRaw.add(id)
		elif (name == modelName and val == "Virtual Machine"):
			vmSetRaw.add(id)
		elif (name == manuName and val == "VMware"):
			vmSetRaw.add(id)
		lstr = ""
fprops.close

#Process vmSetRaw to filter out false positives
for vmId in vmSetRaw:
	#Check class to not include unwanted ones (not Vmware VMs)
	cls = allPropsById[vmId].get(classPropName, '')
	if cls != aixServerStr and cls != esxServerStr and cls != dbSqlStr:
		#print "Adding VM: %s to list" % nodesById[vmId]
		vmSet.add(vmId)

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
		if (hostId, classPropName) not in existingProps:
			props[(hostId, classPropName)] = "cmdb_ci_esx_server"
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
		props[(id, classPropName)] = "cmdb_ci_esx_server"
		props[(id, deviceTypeName)] = "Physical Server"
		props[(id, osName)] = esx
		if domain != '': props[(id, domainName)] = domain

def processVNetwork(cols, row):
	#Find subnet
	server = row[cols[vm]].value.strip()
	if server in nameSwap: server = nameSwap[server]
	ipAddr = row[cols[ipStr]].value.strip()
	powered = row[cols[powerState]].value.strip()
	#print server, ipAddr
	if powered != "poweredOff" and ipAddr != "unknown":
		if ':' or ',' in ipAddr:
			#Remove ipv6 part
			fs = ipAddr.split(',')
			ipAddr = ''
			for f in fs:
				if '.' in f:
					ipAddr = f.strip()
					break
		subnet = ('0', '0', '0')
		if ipAddr != '' : 
			subnet = findSubnet(ipAddr)
			if server.lower() not in nodesByName:
				#New server - add in IP addr to desc
				(id, docStr) = servers[server]
				docStr += "\nIP Address: " + ipAddr
				servers[server] = (id, docStr)
			else:
				id = nodesByName[server.lower()]
			if ipAddr != '' and (id, ipName) not in existingProps:
				props[(id, ipName)] = ipAddr
			rel = (id, "AssociationRelationship", subnet[0])
			if subnet[0] != "0" and rel not in existingRels: 
				netrels.append((id, "AssociationRelationship", subnet[0], ipAddr))

#Process row in spreadsheet
def processVInfo(cols, row):
	server = row[cols[vm]].value.strip()
	osCol = cols.get(osStr, cols.get(osTools))
	if osCol is None: 
		print "Failed to find OS column"
		osystem = ''
	else:
		osystem = row[osCol].value.strip()
	host = row[cols[hostStr]].value.strip().split('.')[0]
	powered = row[cols[powerState]].value.strip()
	fqdn = row[cols[dnsStr]].value.strip()
	dnsName = fqdn.split('.')[0]
	oldName = server
	if fqdn != '' and dnsName.lower() != server.lower():
		id = nodesByName.get(dnsName.lower())
		if id is not None:
			if id in vmSet:
				#Remove found VM from list
				vmSet.remove(id)
			#Use dnsName rather than VM name as the Archi / CMDB name
			print "WARNING: Server %s has a different DNS name: %s, using fqdn name" % (server,fqdn)
			nameSwap[server] = dnsName
			server = dnsName
	if oldName in vmProcessed:
		print "WARNING: SKIPPING Duplicate VM name - already processed %s" % oldName
		return
	vmProcessed.append(oldName)
	cluster = row[cols[clStr]].value.strip()
	cpus = "%d" % row[cols[cpuStr]].value
	ram = "%d GB" % (row[cols[memoryStr]].value/1024)

	#print server, powered, osystem
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
			if osystem != '' :
				if osystem.lower() not in sysSoftware:
					#Add OS to system software
					osid = str(uuid.uuid4())
					softs[osystem] = (osid, osystem) #Add to ones to create
					nodesById[osid] = osystem  #Add to node array to resolve id to name
					sysSoftware[osystem.lower()] = osid #Add to existing node set
					rel = (id, "AssignmentRelationship", osid)
					if rel not in existingRels : rels.append(rel)
				else:
					rel = (id, "AssignmentRelationship", sysSoftware[osystem.lower()])
					if rel not in existingRels : rels.append(rel)
			if (id, classPropName) not in existingProps:
				if "windows" in osystem.lower():
					props[(id, classPropName)] = "cmdb_ci_win_server"
				elif "linux" in osystem.lower():
					props[(id, classPropName)] = "cmdb_ci_linux_server"
				elif "solaris" in osystem.lower():
					props[(id, classPropName)] = "cmdb_ci_solaris_server"
				elif "aix" in osystem.lower():
					props[(id, classPropName)] = "cmdb_ci_aix_server"
				else:
					props[(id, classPropName)] = "cmdb_ci_server"
			if (id, deviceTypeName) not in existingProps \
					or existingProps[(id, deviceTypeName)] != "Virtual Server":
				props[(id, deviceTypeName)] = "Virtual Server"
			if osystem.strip() != '' and ((id, osName) not in existingProps \
					or existingProps[(id, osName)] != osystem):
				props[(id, osName)] = osystem
			if (id, opStatusName) not in existingProps \
					or existingProps[(id, opStatusName)] == "Powered Off" \
					or existingProps[(id, opStatusName)] == "Disposed":
				props[(id, opStatusName)] = "Live"
			if domain.strip() != '' and ((id, domainName) not in existingProps \
					or existingProps[(id, domainName)] != domain):
				props[(id, domainName)] = domain
			if id in vmSet:
				#Remove found VM from set
				#print "Found Vm %s with id %s" % (server, id)
				vmSet.remove(id)
			elif oldName == server: # Dont add props if using fqdn name
				print "VM %s not in Archie Vm list - setting virtual server properties" % server
				props[(id, deviceTypeName)] = "Virtual Server"
				props[(id, modelName)] = "Virtual Machine"
				props[(id, manuName)] = "VMware"
		else :
			docStr = "Vmware VM\nOperating System: " + osystem
			docStr += "\n%s vCPU %s RAM" % (cpus, ram)
			docStr += "\nvCluster: " + cluster
			id = str(uuid.uuid4())
			servers[server] = (id, docStr)
			nodesById[id] = server
			if hostId != "" :
				rels.append((hostId, "CompositionRelationship", id))
			if "windows" in osystem.lower():
				props[(id, classPropName)] = "cmdb_ci_win_server"
			elif "linux" in osystem.lower():
				props[(id, classPropName)] = "cmdb_ci_linux_server"
			elif "solaris" in osystem.lower():
				props[(id, classPropName)] = "cmdb_ci_solaris_server"
			elif "aix" in osystem.lower():
				props[(id, classPropName)] = "cmdb_ci_aix_server"
			else:
				props[(id, classPropName)] = "cmdb_ci_server"
			props[(id, deviceTypeName)] = "Virtual Server"
			if osystem != '': props[(id, osName)] = osystem
			props[(id, opStatusName)] = "Live"
			if osystem.lower() not in sysSoftware:
				#Add OS to system software
				osid = str(uuid.uuid4())
				softs[osystem] = (osid, osystem) #Add to ones to create
				nodesById[osid] = osystem  #Add to node array to resolve id to name
				sysSoftware[osystem.lower()] = osid #Add to existing node set
				rel = (id, "AssignmentRelationship", osid)
				rels.append(rel)
			else:
				rel = (id, "AssignmentRelationship", sysSoftware[osystem.lower()])
				rels.append(rel)
			if domain != '':
				props[(id, domainName)] = domain
	else:
		if server.lower() in nodesByName:
			id = nodesByName[server.lower()]
			#Remove from vmSet as its only poweredoff (and not decommissioned)
			if id in vmSet:
				#Remove found VM from list
				vmSet.remove(id)
			elif oldName == server: # Dont add props if using fqdn name
				print "Powered off VM %s not in Archie Vm list - setting virtual server properties" % server
				props[(id, deviceTypeName)] = "Virtual Server"
				props[(id, modelName)] = "Virtual Machine"
				props[(id, manuName)] = "VMware"
			#Check Operational status and set to powered off
			state = existingProps.get((id, opStatusName), None)
			if state != "Powered Off":
				print "Setting %s to Powered off - currently set to %s" % (server, state)
				props[(id, opStatusName)] = "Powered Off"
				desc = nodeDescByName[server.lower()].strip('"')
				#Amend desc
				newDesc = "Note: VM powered off\n%s" % desc
				servers[server] = (id, newDesc)
			monitored = existingProps.get((id, isMonitoredName), None)
			if monitored.lower() == "true" : props[(id, isMonitoredName)] = "FALSE"
				
def rowToCsv(row):
	out = ""
	for c in row:
		out += str(c.value)
		out += ","
	return out

#Process each RVTools workbook
for file in os.listdir('.'):
	if file.startswith("RVTools_") and (file.endswith(".xls") or file.endswith(".xlsx")):
		print "Processing RVtools file %s" % file
		wb = xlrd.open_workbook(file)
		#Process Vmware hosts
		try:
			ws = wb.sheet_by_name('tabvHost')
		except xlrd.XLRDError as x:
			try:
				ws = wb.sheet_by_name('vHost')
			except xlrd.XLRDError as x:
				print "Error reading Workbook %s: %s" % (file, x)
				continue
		count = 0
		for row in ws.get_rows():
			count += 1
			if count == 1:
				rowCsv = rowToCsv(row)
				cols = processHeader(rowCsv)
			else:
				processVHost(cols, row)
		#Process VMs
		try:
			ws = wb.sheet_by_name('tabvInfo')
		except xlrd.XLRDError as x:
			try:
				ws = wb.sheet_by_name('vInfo')
			except xlrd.XLRDError as x:
				print "Error reading Workbook %s: %s" % (file, x)
				continue
		count = 0
		for row in ws.get_rows():
			count += 1
			if count == 1:
				rowCsv = rowToCsv(row)
				cols = processHeader(rowCsv)
				#print cols
			else:
				processVInfo(cols, row)
		#Process VM network info
		try:
			ws = wb.sheet_by_name('tabvNetwork')
		except xlrd.XLRDError as x:
			try:
				ws = wb.sheet_by_name('vNetwork')
			except xlrd.XLRDError as x:
				print "Error reading Workbook %s: %s" % (file, x)
				continue
		count = 0
		for row in ws.get_rows():
			count += 1
			if count == 1:
				rowCsv = rowToCsv(row)
				cols = processHeader(rowCsv)
			else:
				processVNetwork(cols, row)

#Proces remaining vmSet - these are VMs that are not in RVtools and so should be marked as decommmissioned
print "Processing Archi list of VMs - decommissioning ones that are not in RvTools"
for id in vmSet:
	name = nodesById[id]
	#print "%s server not in RVtools list - marking as decommissioned" % name
	#Check Operational status
	if name.startswith("NIE-TH-TLG") or name.startswith("redhat90") or name.lower().startswith("srv-hps-nie-ercgb"): continue  # Ignore tooling server until it is managed by the cloud team
	state = existingProps.get((id, opStatusName), None)
	if state != "Disposed" and state != "Decommissioned":
		print "Setting %s to Disposed" % name
		props[(id, opStatusName)] = "Disposed"
		props[(id, isMonitoredName)] = "FALSE"
		props[(id, statusName)] = "Retired"
		desc = nodeDescByName[name.lower()].strip('"')
		#Amend desc
		newDesc = "Note: DECOMMISSIONED SERVER\n%s" % desc
		servers[name] = (id, newDesc)
	

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

