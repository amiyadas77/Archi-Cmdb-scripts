#Determines all applications and what system software it uses
#For each system software it includes the end of support dates and a list of servers it is installed on for that application
#Can be used to flag up EoL / EoS issues for systems / applications
#Also spits out for each server the applications that it serves
#Author: Danny Andersen

import sys
import uuid
import csv
from cmdbconstants import *

depends = dict() #Keyed by dependant (parent), set of (dependency (child), relationship)
allRels = set() #Set of all relationships (parent, rel, child) 
appRelSet = set() #Set of dependencies (parent, relationship, child) for an application
classByNode = dict() # Keyed by node id, the Cmdb class of the node
appsList = list() # list of application ids
serverList = set() # set of server ids
appsByServer = dict() # list of application ids keyed by server id
vCluster = set() # set of TechnologyCollaboration that are a Vmware cluster
serverVcluster = dict() # Keyed by serverId, the vClusterId it belongs to
sysSoftware = list() # list of system software ids
appsChildren = dict() # Keyed by apps id, the set of children 
appsSoftwareWithNodes = dict() # Keyed by "(apps id, child id)" the set of nodes with that combination (i.e. node is child of an app and hosts the software) 

#Add dependencies for passed in id, recursing down the tree - each dependency keyed by dependant, set of (dependency, outage, relationship)
def addDepend(id, subId):  #Note: Id = Super parent, subId = add all subIds as children
	global appRelSet
	global depends
	global allowedRels
	#print "%s: Adding children of %s" % (nodesById[id], nodesById[subId])
	rels = depends.get(subId, set())
	for rel in rels:
		type = rel[2]
		child = rel[0]
		#print rel, 
		#print "%s, %s, %s" % (nodesById[child], nodesById[subId], nodesById[rel[0]])
		#print "%s,%s,%s" % (nodesById[child], nodesById[subId], rel)
		newRel = (id, type, child)
		appRelSet.add((newRel[0], newRel[1], newRel[2]))
		addDepend(id, child)

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
	name = fs[2].strip('"')
	lowerName = name.lower()
	id = fs[0].strip('"')
	nodesByName[lowerName] = id
	nodeType = fs[1].strip('"')
	nodesById[id] = (name, nodeType)
	if nodeType == "TechnologyCollaboration" and "vcluster" in lowerName:
		vCluster.add(id)
	#if nodeType == "ApplicationComponent":
	#	appsList.append(id)
	
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
		#Pull out appsList
		if name == classPropName:
			classByNode[id] = val
			if val == appStr and nodesById[id][1] == "ApplicationComponent" : appsList.append(id)
		#Pull out servers
		if name == classPropName:
			classByNode[id] = val
			if val in serverClasses : serverList.add(id)
	lstr = ""
fprops.close

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
		if type == "ServingRelationship" or type == "RealizationRelationship":
			rel = (srcId, targetId, type)
			if targetId in depends: 
				depends[targetId].add(rel)
			else: 
				depends[targetId] = set([rel])
		elif type == "AssignmentRelationship" or type == "AggregationRelationship" or type == "CompositionRelationship":
			if srcId in vCluster and targetId in serverList:
				serverVcluster[targetId] = srcId
			rel = (targetId, srcId, type)
			if srcId in depends: 
				depends[srcId].add(rel)
			else: 
				depends[srcId] = set([rel])
		allRels.add(rel)
		#print rel
		lstr = ""
frels.close

#For each app :
#Find all "serving" rels that have target of node, add them to dependency set
#Find their child "serving" rels and add them to the dependency set 

for appId in appsList:
	addDepend(appId, appId)

print "Apps list %d, depends: %d" % (len(appsList), len(depends))

#Sort appsRels by apps
#with Unique set of children
for d in appRelSet:
	parent = d[0]
	child = d[2]
	#print "app: %s" % (nodesById[parent][0])
	if parent in appsChildren:
		appsChildren[parent].add(child)
		#print "app: %s children: %d" %(nodesById[parent][0], len(children))
	else:
		appsChildren[parent] = set([child])

#Find children that is an application, roll up its system software to parent and dont add to final list
repeat = True
while repeat:
	repeat = False
	for app in appsChildren:
		children = appsChildren[app]
		for child in children:
			#Check if child is an app and is a child of this app and is in a Composition relationship with the parent
			if child in appsList and child in appsChildren and (app, "CompositionRelationship", child) in appRelSet:
				#Add child's children to parent and delete from dict
				print "Adding to app: %s children of: %s" % (nodesById[app][0], nodesById[child][0])
				children = children.union(appsChildren[child])
				del appsChildren[child]
				repeat = True
				break
			#Filter system software that is a child of other system software that is also a child of this app
			if nodesById[child][1] == "SystemSoftware":
				for child2 in children:
					childType = nodesById[child2][1]
					if  childType == "SystemSoftware":
						# if (nodesById[child2][0] == "Office 2010"):
							# print "%s %s %s" % (nodesById[child2][0], "AggregationRelationship", nodesById[child][0])
						if (child2, child, "AggregationRelationship") in allRels:
							#print "Removing %s" % nodesById[child2][0]
							children.remove(child2)
							repeat = True
							break
					elif childType == "Node" or childType == "Grouping" or childType == "Device":
						#Add nodes with system software to sublist for system sofware 
						if (child, child2, "AggregationRelationship") in allRels or (child, child2, "AssignmentRelationship") in allRels:
							#(Node is in child relationship with app, system software in aggregation or assigned relationship with node)
							if (app, child) in appsSoftwareWithNodes: 
								nodes = appsSoftwareWithNodes[(app, child)]
							else: 
								nodes = set()
								appsSoftwareWithNodes[(app, child)] = nodes
							#print "Adding node %s" % nodesById[child2][0]
							nodes.add(nodesById[child2][0])
				if repeat: break
			if child in serverList:
				#Add application to server apps list
				if child in appsByServer:
					apps = appsByServer[child]
					if app not in apps:
						apps.append(app)
				else:
					appsByServer[child] = [app]
		if repeat: break

freadable = open("apps-software-eol.csv", "w")
print >>freadable, "Application, Software, End of Life, End of Service Life, Servers"
for app in appsChildren:
	#operation,p_unique_id,p_class,p_name,p_company,type,c_unique_id,c_class,c_name,c_company,connection_strength,percent_outage,u_schedule,,,,,,,
	# d= (parent, relationship, child, strength, outage)
	parent = app
	for child in appsChildren[app]:
		if nodesById[child][1] == "SystemSoftware":
			eolDate = props.get((child, eolDateName), '')
			eoslDate = props.get((child, eoslDateName), '')
			nodeSet = appsSoftwareWithNodes.get((app, child), set())
			nodeStr = ""
			for node in nodeSet:
				nodeStr += "%s;" % node
			print >>freadable, '%s,%s,%s,%s,%s' % (nodesById[app][0], nodesById[child][0],eolDate, eoslDate, nodeStr)
		#print >>freadable, '%s,%s,%s,%s' % (nodesById[app][0],nodesById[app][1], nodesById[child][0],nodesById[child][1])
freadable.close

freadable = open("serversAndApps.csv", "w")
print >>freadable, "Server, Op Status, Type, Operating System, IP Address, vCluster, Applications"
for serverId in appsByServer:
	status = props.get((serverId, opStatusName), '')
	type = revClassLookup.get(props.get((serverId, classPropName),'None'), 'None')
	#(cpu, mem) = extractCPUMem(serverId)
	#deviceType = props.get((serverId, deviceTypeName), '')
	#model = props.get((serverId, modelName), '')
	opSystem = props.get((serverId, osName), '')
	ipAddress = props.get((serverId, ipName), '')
	vClusterId = serverVcluster.get(serverId, 'None')
	vCluster = nodesById.get(vClusterId, ('None', ''))[0]
	appStr = ""
	first = True
	for app in appsByServer[serverId]:
		if not first: 
			appStr += "; "
		else:
			first = False
		appStr += "%s" % (nodesById[app][0])
	print >>freadable, '%s,%s,%s,%s,%s,%s,%s' % (nodesById[serverId][0], status, type, opSystem, ipAddress, vCluster, appStr)
freadable.close
