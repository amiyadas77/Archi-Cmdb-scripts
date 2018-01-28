#Archie EA tool to Snow CMDB synchronisation script
#Creates New/Changed/Deleted CMDB relationship import file based on all archie relationships that comply with the SNOW CMDB data model
#Note that the mapping between an Archimate relationship and a CMDB relationship is held in a spreadsheet called "rel-rules.csv".
#Only relationships appearing in this file will be mapped to CMDB
#Author: Danny Andersen

import sys
import uuid

allowedRels = dict() # Keyed by (classFrom, classTo, ArchieRelationship) value = (CMDB Rel, parent->child = True)
props = dict() #Keyed by (node id, property name)
nodesByName = dict() #Keyed by node name, id of node
cmdbIdByName = dict() #Keyed by name, cmdb id of node
nodesById = dict() # Keyed by node id, name of node
classByNode = dict() # Keyed by node id, the Cmdb class of the node
archieIdtoCmdbId = dict() #keyed by Archie node id, cmdb id
existingCmdbRels = dict() #Dependencies keyed by tuple (parent, relationship, child) with value of (strength, outage)
existingCmdbRelsComplete = set() #Dependencies keyed by (parent, relationship, child, strength, outage)
cmdbRelSet = set() #New/changed Set of dependencies (action, parent, relationship, child, strength, outage)
allFullCmdbSet = set() #All CMDB relationships keyed by (parent, relationship, child, strength, outage)
allShortCmdbSet = set() #All short CMDB relationships keyed by (parent, relationship, child)
depends = dict() #Keyed by dependant (parent), set of (dependency (child), relationship, strength, outage)

missingFromCmdb = set() # Set of node names missing from CMDB
missingRels = set() # Set of archie class relationships not in data model (i.e. rules csv file)

classPropStr = "CMDB Class"
cmdbIdStr = "CMDB ID"

osName = "CMDB Operating System"
strengthPropStr = "CMDB-REL Strength"
outagePropStr = "CMDB-REL Outage"
deviceTypeName = "CMDB Device Type"
osName = "CMDB Operating System"
fnName = "CMDB Function"
ipName = "CMDB IP Address"
statusName = "CMDB Status"

alwaysStr = "Always"
clusterRelStr = "Cluster"
occStr = "Occasional"
infreqStr = "Infrequent"

company = "NIE Networks"

createOperation = "create"
updateOperation = "update"
deleteOperation = "delete"

passThruStr = "PASS-THRU"

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
			cmdbIdByName[nodesById[id]] = val
		
		lstr = ""
fprops.close

#Read in existing CMDB relationships
fcmdb = open("CMDB relations.csv")
count = 0
for line in fcmdb:
	count += 1
	if count == 1: continue
	skip = False
	fs = line.rstrip('\n\r').split(",")
	parent = fs[1].strip('"')
	if len(fs) != 7:
		print "Warning: Import file has a row (parent name = %s) of the incorrect length: %d" % (parent, len(fs))
		skip = True
	parentId = nodesByName.get(parent.lower())
	type = fs[2].strip('"').strip()
	child = fs[4].strip('"').strip().lower()
	childId = nodesByName.get(child.lower())
	strength = fs[5].strip()
	outage = fs[6].strip()
	if parentId == None:
		print "Warning: CMDB CI %s not found in Archie - run sync on new CI extract" % parent
		skip = True
	if childId == None:
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
for relation in existingCmdbRels:
	#Test each relationship in the CMDB set and create deletion entries if no longer existing in Archie
	if relation not in allShortCmdbSet:
		#Relationship deleted in archie - delete in CMDB
		(strength, outage) = existingCmdbRels[relation]
		cmdbRelSet.add((deleteOperation, relation[0], relation[1], relation[2], strength, outage))
	
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
	#print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (nodesById[parent],parentClass, nodesById[child],childClass,childOs,osFamily,devType,fn,ipAddress, d[1],d[3],d[4])
	print >>freadable, '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (action, nodesById[parent],parentClass, nodesById[child],childClass, type,strength,outage, parentMissing, childMissing)
	#Use the following for actual export to CMDB
	if not childMissing and not parentMissing:
		print >>frels, '%s,%s,,,,%s,%s,,,%s,%s,' % (action,archieIdtoCmdbId[parent],type,archieIdtoCmdbId[child],strength,outage)
frels.close	
freadable.close

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
