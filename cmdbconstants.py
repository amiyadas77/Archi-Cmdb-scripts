props = dict() #Keyed by (node id, property name)
cmdb = dict() #Keyed by Name, the cmdb id
newProps = list()
cmdbProps = dict() #CMDB props from CMDB file, keyed by cmdb id + property name
nodesByName = dict() #Keyed by node name, id of node
nodesById = dict() # Keyed by node id, name of node

parentName = "Parent"
typeName = "Type"
childName = "Child"
strengthName = "Connection strength"
outageName = "Percent outage"

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
fwStr = "cmdb_ci_ip_firewall"
fwClass = "IP Firewall"

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
assetTagName = "CMDB Asset Tag"

propNameSet = {classPropName, deviceTypeName, osName, fnName, ipName, manuName, modelName, \
					locationName, criticalityName, serviceClassName, installName, \
					statusName, serialName, opStatusName, domainName, \
					monitorObName, monitorToolName, isMonitoredName, assetTagName}
			
propLookup = {"Unique ID": cmdbIdName, "Class": classPropName, "Device Type": deviceTypeName, \
					
					"OS Version": osName, "Function Type": fnName, \
					"IP Address": ipName,  "Manufacturer": manuName, "Model ID": modelName, \
					"Location": locationName, "Criticality": criticalityName, "Service classification": serviceClassName, \
					"Installation Path": installName, "Status": statusName, "Serial number": serialName, \
					"Operational status": opStatusName, "DNS Domain": domainName, \
					"Monitoring Object ID": monitorObName, "Monitoring Tool": monitorToolName, "Is Monitored": isMonitoredName, \
					"Asset tag": assetTagName}

propRevLookup = dict()
for p in propLookup:
	propRevLookup[propLookup[p]] = p
	
#Process header line and return a dict keyed by column name, with value of field number	
def processHeader(headerLine):
	cols = headerLine.strip('\n\r').split(',')
	colDict = dict()
	num = 0;
	for col in cols:
		colDict[col.strip()] = num
		num += 1
	return colDict

#Process header line and return a of each header	
def getPropList(headerLine):
	cols = headerLine.strip('\n\r').split(',')
	return cols

