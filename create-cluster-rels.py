#One off script to create cluster relationships for nodes that have many connections (e.g. Storage switches)

classPropStr = "CMDB Class"
strengthPropStr = "CMDB-REL Strength"
outagePropStr = "CMDB-REL Outage"
deviceTypeStr = "CMDB Device Type"
clusterStr = "Cluster"
virtualStr = "Virtual Server"
physicalStr = "Physical Server"

alwaysStr = "Always"
clusterStr = "Cluster"
occStr = "Occasional"
infreqStr = "Infrequent"

appStr = "cmdb_ci_appl"
businessStr = "cmdb_ci_service"
serverStr = "cmdb_ci_server"
esxServerStr = "cmdb_ci_esx_server"
aixServerStr = "cmdb_ci_aix_server"
linuxStr = "cmdb_ci_linux_server"
winStr = "cmdb_ci_win_server"
vmStr = "cmdb_ci_vmware_instance"
storageSwStr = "cmdb_ci_storage_switch"

servingStr = "ServingRelationship"
compositionStr = "CompositionRelationship"
specialStr = "SpecialisationRelationship"
company = "NIE Networks"

existingRels = dict() #Key = (parent, type, child), val = rel id
relsByNodeId = dict() # Key = node id, val = list() of (parent, type, child)
existingProps = dict() #Keyed by (node id, property name)
newProps = list() #List of (node id, property name, val)

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
		parent = fs[4].strip('"')
		type = fs[1].strip('"')
		child = fs[5].strip('"')
		relKey = (parent, type, child)
		existingRels[relKey] = fs[0].strip('"')
		if parent in relsByNodeId:
			relsByNodeId[parent].append(relKey)
		else :
			relsByNodeId[parent] = list([relKey])
		if child in relsByNodeId:
			relsByNodeId[child].append(relKey)
		else :
			relsByNodeId[child] = list([relKey])
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
		#propsById.get(id,list()).append((name, val))
		lstr = ""
fprops.close

#Change all esx-server relationships with vm instances to Strength = Cluster Outage = 0
#Find all nodes with cmdb class = esx-server
#For each esx-server find all relations that are to vminstances
#If strength prop not set on relation, set to cluster and set outage property
for propKey in existingProps:
	id = propKey[0]
	name = propKey[1]
	val = existingProps[propKey]
	if name == classPropStr:
		if val == esxServerStr:
			#print "ESX server: " + id
			rels = relsByNodeId.get(id, list())
			for rel in rels:
				parent = rel[0]
				type = rel[1]
				child = rel[2]
				#Check if child a vm instance
				ciclass = existingProps.get((child, classPropStr), '')
				devType = existingProps.get((child, deviceTypeStr), '')
				if ciclass == vmStr or devType == virtualStr:
					#print "VM server: " + child
					#Check rel properties are set
					relId = existingRels[rel]
					val = existingProps.get((relId, strengthPropStr), '')
					if val != clusterStr: newProps.append((relId, strengthPropStr, clusterStr))
					val = existingProps.get((relId, outagePropStr), '')
					if val != "0": newProps.append((relId, outagePropStr, "0"))
		#Check if a Brocade switch, if so, relationship should be a cluster
		if val == storageSwStr:
			rels = relsByNodeId.get(id, list())
			for rel in rels:
				parent = rel[0]
				type = rel[1]
				child = rel[2]
				#Check if child is a virtual or physical server
				devType = existingProps.get((child, deviceTypeStr), '')
				if devType == virtualStr or devType == physicalStr:
					relId = existingRels[rel]
					p = existingProps.get((relId, strengthPropStr), '')
					if p != clusterStr: newProps.append((relId, strengthPropStr, clusterStr))
					p = existingProps.get((relId, outagePropStr), '')
					if p == '': newProps.append((relId, outagePropStr, "50"))
				
felems = open("new-elements.csv", "w")
print >>felems,'"ID","Type","Name","Documentation"'					
felems.close
frels = open("new-relations.csv", "w")
print >>frels,'"ID","Type","Name","Documentation","Source","Target"'
frels.close

fprops = open("new-properties.csv", "w")
print >>fprops,'"ID","Key","Value"'
for prop in newProps:
	print >>fprops, '"%s","%s","%s"' % (prop[0], prop[1], prop[2])
fprops.close	
