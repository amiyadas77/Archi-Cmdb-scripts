#Archie EA tool to Snow CMDB synchronisation script
#Creates New/Changed/Deleted CMDB relationship import file based on all archie relationships that comply with the SNOW CMDB data model
#Note that the mapping between an Archimate relationship and a CMDB relationship is held in a spreadsheet called "rel-rules.csv".
#Only relationships appearing in this file will be mapped to CMDB
#Author: Danny Andersen

import sys
import uuid
import csv
from cmdbconstants import *

allowedRels = dict() # Keyed by (classFrom, classTo, ArchieRelationship) value = (CMDB Rel, parent->child = True)
cmdbToArchiRels = dict() # Keyed by (classFrom, classTo, CMDB Rel) value = (ArchieRelationship, parent->child = True)
classByNode = dict() # Keyed by node id, the Cmdb class of the node
archieIdtoCmdbId = dict() #keyed by Archie node id, cmdb id
existingCmdbRels = dict() #Dependencies keyed by tuple (parent, relationship, child) with value of (strength, outage)
existingCmdbRelsComplete = set() #Dependencies keyed by (parent, relationship, child, strength, outage)
cmdbRelSet = set() #New/changed Set of dependencies (action, parent, relationship, child, strength, outage)
allFullCmdbSet = set() #All CMDB relationships keyed by (parent, relationship, child, strength, outage)
allShortCmdbSet = set() #All short CMDB relationships keyed by (parent, relationship, child)
depends = dict() #Keyed by dependant (parent), set of (dependency (child), relationship, strength, outage)
newArchiRels = set() # Set on new relationships to add to Archi

missingFromCmdb = set() # Set of node names missing from CMDB
missingRels = set() # Set of archie class relationships not in data model (i.e. rules csv file)

#Add dependencies for passed in id, recursing down the tree - each dependency keyed by dependant, set of (dependency, outage, relationship)
def addDepend(id, subId, inType):  #Note: Id = Super parent, subId = add all subIds as children
	global fullCmdbSet
	global cmdbRelSet
	global depends
	print "%s: Pass thru - adding children of %s" % (nodesById[id], nodesById[subId])
	srcClass = classByNode.get(id, 'NONE')
	rels = depends.get(subId, set())
	for rel in rels:
		#print "%s,%s,%s" % (nodesById[id], nodesById[subId], nodesById[rel[0]])
		#print "%s,%s,%s" % (nodesById[id], nodesById[subId], rel)
		#cmdbRel -> (parent, relationship, child, strength, outage)
		#rel => (dependency (child), relationship, strength, outage)
		type = rel[1]
		child = rel[0]
		targetClass = classByNode.get(targetId, 'NONE')
		relByClass = (srcClass, targetClass, type)
		if relByClass in allowedRels or srcClass == 'NONE' or targetClass == 'NONE': 
			if type == passThruStr or srcClass == 'NONE' or targetClass == 'NONE':
				print "%s: Pass thru - of a passthru adding children of : %s" % (nodesById[id], nodesById[child])
				addDepend(id, child) #pass thru of a pass thru - add childrens children
			else:
				newRel = (id, type, child, rel[2], rel[3])
				shortRel = (id, type, child)
				allFullCmdbSet.add(newRel) #Add to a complete set of all relations to check for deletions
				allShortCmdbSet.add(shortRel) #Add to a complete set of all relations to check for deletions
				if shortRel not in existingCmdbRels:
					cmdbRelSet.add((createOperation, newRel[0], newRel[1], newRel[2], newRel[3], newRel[4]))
				elif newRel not in existingCmdbRelsComplete:
					#Update to strength / outage, rather than create
					cmdbRelSet.add((updateOperation, newRel[0], newRel[1], newRel[2], newRel[3], newRel[4]))

					#Process header line and return a dict keyed by column name, with value of field number	

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
	type = fs[3].strip()
	keepDirRel = fs[4].strip().lower() == 'true'
	allowedRels[(srcRel, targetRel, archieRel)] = (type, keepDirRel)
	cmdbToArchiRels[(srcRel, targetRel, type)] = (archieRel, keepDirRel)
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
		if name == cmdbIdStr: 
			archieIdtoCmdbId[id] = val
		
		lstr = ""
fprops.close

#Read in CMDBs to check status
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
	
	cmdbId = fields[cols[propRevLookup[cmdbIdName]]]
	classField = fields[cols[propRevLookup[classPropName]]]
	status = fields[cols[propRevLookup[statusName]]]
	opStatus = fields[cols[propRevLookup[opStatusName]]].strip()
	#print cmdbId, classField, status]
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
		if status != '' : cmdbProps[(cmdbId, statusName)] = status
		if opStatus != '' : cmdbProps[(cmdbId, opStatusName)] = opStatus

fcmdb.close

#Read in existing CMDB relationships
fcmdb = open("CMDB relations.csv")
count = 0
for line in fcmdb:
	count += 1
	if count == 1:
		cols = processHeader(line)
		continue
	skip = False
	fs = line.rstrip('\n\r').split(",")
	parent = fs[cols[parentName]].strip('"')
	if len(fs) < 7:
		print "Warning: Import file has a row (parent name = %s) that is too small: %d" % (parent, len(fs))
		skip = True
	parentId = nodesByName.get(parent.lower())
	type = fs[cols[typeName]].strip('"').strip()
	child = fs[cols[childName]].strip('"').strip().lower()
	childId = nodesByName.get(child.lower())
	strength = fs[cols[strengthName]].strip()
	outage = fs[cols[outageName]].strip()
	if parentId == None:
		cmdbId = cmdb[parent.lower()]
		cmdbStatus = cmdbProps.get((cmdbId, statusName), '').strip()
		cmdbRetired = cmdbStatus == "Retired" or cmdbStatus == "Absent" or cmdbStatus == "Disposed"
		if not cmdbRetired:
			print "Warning: CMDB CI %s not found in Archie - run sync on new CI extract" % parent
		skip = True
	if childId == None:
		cmdbId = cmdb[child.lower()]
		cmdbStatus = cmdbProps.get((cmdbId, statusName), '').strip()
		cmdbRetired = cmdbStatus == "Retired" or cmdbStatus == "Absent" or cmdbStatus == "Disposed"
		if not cmdbRetired:
			print "Warning: CMDB CI %s not in Archie - run sync on new CI extract" % child
		skip = True
	if not skip:
		rel = (parentId, type, childId, strength, outage)
		shortRel = (parentId, type, childId)
		existingCmdbRelsComplete.add(rel)
		existingCmdbRels[shortRel] = (strength, outage)
fcmdb.close

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
		srcClass = classByNode.get(srcId, 'NONE')
		targetClass = classByNode.get(targetId, 'NONE')
		strength = props.get((relId, strengthPropStr), alwaysStr)
		outage = "100"
		if strength == clusterRelStr:
			outage = props.get((relId, outagePropStr), "0")
		if srcClass == 'NONE' or targetClass == 'NONE': 
			relByClass = ('NONE', 'NONE', type)
		else: 
			relByClass = (srcClass, targetClass, type)
		if relByClass in allowedRels: 
			#Relationship is allowed by datamodel or a passthru
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
		srcClass = classByNode.get(srcId, 'NONE')
		targetClass = classByNode.get(targetId, 'NONE')
		outage = "100"
		strength = props.get((relId, strengthPropStr), alwaysStr)
		if strength == clusterRelStr:
			outage = props.get((relId, outagePropStr), "0")
		relByClass = (srcClass, targetClass, type)
		if relByClass in allowedRels: 
			#Relationship is allowed by datamodel
			cmdbRelTuple = allowedRels[relByClass]
			cmdbRelType = cmdbRelTuple[0]
			keepDirection = cmdbRelTuple[1]
			passThru = cmdbRelType == passThruStr
			#Add relationship (parent, relationship, child, strength, outage)
			if keepDirection: # Parent->Child = Src->Target
				if passThru:
					#Need to add in childs relationships directly to the parent, i.e. skip the intermediate
					addDepend(srcId, targetId, type)
				else:
					newRel = (srcId, cmdbRelType, targetId, strength, outage)
					shortRel = (srcId, cmdbRelType, targetId)
					allFullCmdbSet.add(newRel) #Add to a complete set of all relations to check for deletions
					allShortCmdbSet.add(shortRel) #Add to a complete set of all relations to check for deletions
					if shortRel not in existingCmdbRels:
						cmdbRelSet.add((createOperation, newRel[0], newRel[1], newRel[2], newRel[3], newRel[4]))
					elif newRel not in existingCmdbRelsComplete:
						#Update to strength / outage, rather than create
						cmdbRelSet.add((updateOperation, newRel[0], newRel[1], newRel[2], newRel[3], newRel[4]))
			else: # Parent->Child = Target->Src
				if passThru: 
					#Need to add in childs relationships directly to the parent
					addDepend(targetId, srcId, type)
				else:
					newRel = (targetId, cmdbRelType, srcId, strength, outage)
					shortRel = (targetId, cmdbRelType, srcId)
					allFullCmdbSet.add(newRel) #Add to a complete set of all relations to check for deletions
					allShortCmdbSet.add(shortRel) #Add to a complete set of all relations to check for deletions
					if shortRel not in existingCmdbRels:
						cmdbRelSet.add((createOperation, newRel[0], newRel[1], newRel[2], newRel[3], newRel[4]))
					elif newRel not in existingCmdbRelsComplete:
						#Update to strength / outage, rather than create
						cmdbRelSet.add((updateOperation, newRel[0], newRel[1], newRel[2], newRel[3], newRel[4]))
		else:
			missingRels.add((relByClass, nodesById[srcId], nodesById[targetId]))
			#print "Rel not allowed in Data model: (%s, %s, %s)" % relByClass

		lstr = ""
frels.close

#Check for deletes by checking each CMDB relationships is still in Archie - if not delete it.
for relation in existingCmdbRelsComplete:
	#Test each relationship in the CMDB set and create deletion entries if no longer existing in Archie
	parentId = relation[0]
	childId = relation[2]
	type = relation[1]
	strength = relation[3]
	outage = relation[4]
	shortRel = (parentId, type, childId)
	if shortRel not in allShortCmdbSet:
		#Relationship deleted in archie - delete in CMDB
		#Or relationship needs to be created in Archie....you decide!
		cmdbRelSet.add((deleteOperation, relation[0], relation[1], relation[2], strength, outage))
		print "Relationship %s to %s via %s is not in Archi - check that this is correct or whether relationship needs to be imported into Archi" \
				% (nodesById[parentId], nodesById[childId], type)
		#Retrieve Archi rule
		srcClass = classByNode.get(parentId, 'NONE')
		destClass = classByNode.get(childId, 'NONE')
		#Retrieve equivalent Archi relationship type
		(archRelType, keepDir) = cmdbToArchiRels.get((srcClass, destClass, type), ('NONE', False))
		if archRelType == 'NONE': 
			(archRelType, keepDir) = cmdbToArchiRels.get((destClass, srcClass, type), ('NONE', False))
			if archRelType == 'NONE' or keepDir == True: 
				print "Failed to find relationship for class %s to class %s via type %s, keep %s" % (srcClass, destClass, type, keepDir)
				continue
		id = str(uuid.uuid4())
		if keepDir:
			newArchiRels.add((parentId, archRelType, childId, "", id))
		else:
			newArchiRels.add((childId, archRelType, parentId, "", id))
		if strength != "" and strength != "Always":
			#Add strength and outage props
			newProps.append((id, strengthPropStr, strength))
			newProps.append((id, outagePropStr, outage))

frels = open("cmdb-relations-changes.csv", "w")
freadable = open("changed-readable-relations.csv", "w")
freltemplate = open("cmdb-relations-template.csv")
for t in freltemplate:
	print >>frels,t
freltemplate.close
#print >>freadable, "Parent, Parent Class, Child, Child Class, Child OS, Child OS Family, Child Device, Child Function, Child IP Addr, Relationship, Strength, Outage"
print >>freadable, "Action, Parent, Parent Class, Child, Child Class, Relationship, Strength, Outage, Parent Missing CMDB CI, Child Missing CMDB CI"
for d in cmdbRelSet:
	#operation,p_unique_id,p_class,p_name,p_company,type,c_unique_id,c_class,c_name,c_company,connection_strength,percent_outage,u_schedule,,,,,,,
	# d= (parent, relationship, child, strength, outage)
	action = d[0]
	parent = d[1]
	type = d[2]
	child = d[3]
	strength = d[4]
	outage = d[5]
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
	#Use the following for actual export to CMDB
	if not childMissing and not parentMissing:
		print >>frels, '%s,%s,,,,%s,%s,,,%s,%s,' % (action,archieIdtoCmdbId[parent],type,archieIdtoCmdbId[child],strength,outage)
		print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (action, nodesById[parent],parentClass, nodesById[child],childClass, type,strength,outage, parentMissing, childMissing)
frels.close	
freadable.close

#Create Archi update - for relationships changed / added on CMDB 
#Now update archie
felems = open("new-elements.csv", "w")
print >>felems,'"ID","Type","Name","Documentation"'
felems.close

frels = open("new-relations.csv", "w")
freadable = open("new-relations-readable.csv", "w")
print >>frels,'"ID","Type","Name","Documentation","Source","Target"'
print >>freadable,'"Parent","Child","Relationship"'
for rel in newArchiRels:
	print >>frels, '"","%s","","","%s","%s"' % (rel[1], rel[0], rel[2])
	print >>freadable, '"%s","%s","%s"' % (nodesById[rel[0]], nodesById[rel[2]], rel[1])
frels.close	

fprops = open("new-properties.csv", "w")
print >>fprops,'"ID","Key","Value"'
for prop in newProps:
	print >>fprops, '"%s","%s","%s"' % (prop[0], prop[1], prop[2])
fprops.close

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

freadable = open("all-readable-relations.csv", "w")
#print >>freadable, "Parent, Parent Class, Child, Child Class, Child OS, Child OS Family, Child Device, Child Function, Child IP Addr, Relationship, Strength, Outage"
print >>freadable, "Parent, Parent Class, Child, Child Class, Relationship, Strength, Outage, Parent Missing CMDB CI, Child Missing CMDB CI"
for d in allFullCmdbSet:
	#operation,p_unique_id,p_class,p_name,p_company,type,c_unique_id,c_class,c_name,c_company,connection_strength,percent_outage,u_schedule,,,,,,,
	# d= (parent, relationship, child, strength, outage)
	parent = d[0]
	child = d[2]
	type = d[1]
	strength = d[3]
	outage = d[4]
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
		parentMissing = True
	if child not in archieIdtoCmdbId:
		childMissing = True
	#print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (nodesById[parent],parentClass, nodesById[child],childClass,childOs,osFamily,devType,fn,ipAddress, d[1],d[3],d[4])
	print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s' % (nodesById[parent],parentClass, nodesById[child],childClass, type,strength,outage, parentMissing, childMissing)
freadable.close
