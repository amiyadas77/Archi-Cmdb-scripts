#Archie EA tool to Snow CMDB synchronisation script
#Compares CMDB export with Archie export and creates a CSV file that has the correct attributes for enabling monitoring on the CI.
#Author: Danny Andersen

import sys
import uuid

nodesByName = dict() #Keyed by node name, id of node
existingProps = dict() #Keyed by node id + property name
cmdbUpdate = set() #Update tuple (cmdbid, name, isMonitored, monitorObj, monitor tool)
existingRels = dict() #Key = (parent, type, child), val = rel id

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

appStr = "cmdb_ci_appl"
serverStr = "cmdb_ci_server"
esxServerStr = "cmdb_ci_esx_server"
aixServerStr = "cmdb_ci_aix_server"
dbInstStr = "cmdb_ci_db_instance"
dbOraStr = "cmdb_ci_db_ora_instance"
dbSqlStr = "cmdb_ci_db_mssql_instance"
db2DbStr = "cmdb_ci_db_db2_instance"
sybDbStr = "cmdb_ci_db_syb_instance"
linuxStr = "cmdb_ci_linux_server"
solarisStr = "cmdb_ci_solaris_server"
netStr = "cmdb_ci_netgear"
winStr = "cmdb_ci_win_server"

nagios = "ATF-NAGIOS"
scom = "ATF-SCOM-2"

company = "NIE Networks"

def getHostAndDB(name):
	#Find hostname in name string. Host should be after the first "-"
	host = ""
	db = ""
	type = "engine"
	parts = name.split(" ")
	if len(parts) > 1:
		#Check Reporting / analysis db
		if "analysis" in parts[1].lower():
			type = "analysis"
		elif "reporting" in parts[1].lower():
			type = "reporting"
	nameLen = len(parts[0])
	hostFound = False
	start = 0
	loc = 0
	while not hostFound and start <= nameLen and loc != -1:
		loc = name.find("-", start, nameLen)
		if loc != -1:
			host = name[loc+1:nameLen]
			start = loc + 1
			#Check hostname
			if host in nodesByName:
				#Valid host. Check relationship
				rel = (nodesByName[host], "ServingRelationship", nodesByName[name])
				if rel in existingRels:
					hostFound = True
					db = name[0:loc]
				else:
					print "Found host %s but not serving relationship to %s" % (host, name)
			else:
				print "Didnt find default host %s for %s" % (host, name)
		print hostFound, start, nameLen, loc		
	if not hostFound:
		print "Host not found in name for %s" % name
	else:
		print host, db, type
	return (host, db, type)
	
#Read in Archie nodes from exported file to create dict of names
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
	nodesByName[name] = id
fnodes.close

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
	#Get class and generate monitoring id
	cmdbClass = existingProps.get((id, classPropName))
	cmdbId = existingProps.get((id, cmdbIdName))
	domain = existingProps.get((id, domainName), "")
	isMonitored = False
	moPost = ""
	monitorHostname = name
	if cmdbClass == aixServerStr or cmdbClass == linuxStr or cmdbClass == solarisStr:
		monitoringTool = nagios
		mp = "os-"
		isMonitored = True
	elif cmdbClass == winStr:
		monitoringTool = scom
		mp = "os-"
		isMonitored = True
	elif cmdbClass == dbSqlStr:
		monitoringTool = scom
		mp = "msq-"
		(host, db, type) = getHostAndDB(name)
		if db != "":
			moPost = "\\" + db
			monitorHostname = host #Change the name to the host that needs to be monitored
			hostId = nodesByName[monitorHostname]
			domain = existingProps.get((hostId, domainName), "")
			isMonitored = True
	if isMonitored:
		if domain != "": domain = "." + domain
		monitorObj = (mp + name + domain + moPost)
		# Check to see if already set correctly
		cmdbIsMonitored = existingProps.get((id, isMonitoredName), "false").lower()
		cmdbMonitorObj = existingProps.get((id, monitorObName))
		cmdbTool = existingProps.get((id, monitorToolName))
		change = False
		if cmdbIsMonitored == "false" or cmdbMonitorObj == None or cmdbTool == None:
			change = True
		if cmdbIsMonitored == "true" and cmdbMonitorObj.lower() != monitorObj.lower():
			print "Monitoring object id not correct for %s, changing from %s to %s" % (name, cmdbMonitorObj, monitorObj)
			change = True
		if change:
			cmdbUpdate.add((cmdbId, name, isMonitored, monitorObj, monitoringTool))
	#if cmdbClass == esxServerStr:
		
fnodes.close
#print "Existing nodes: " + str(count) + ": " + str(len(nodes))

fprops = open("new-cmdb-props.csv", "w")
print >>fprops,'"Unique ID","Name", "Is Monitored","Monitoring Object ID", "Monitoring Tool"'
for update in cmdbUpdate:
	print >>fprops, '%s,%s,%s,%s,%s' % (update[0], update[1], update[2], update[3], update[4])
fprops.close	

