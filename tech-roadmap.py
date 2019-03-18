#Reads in the Atos Technology roadmap spreadsheet and processes it against the Archi system software and apps
#Author: Danny Andersen

#TODO
# Match tech to System software in archi export

# Step 1 Tech roadmap
# Process each tab: for each row where A col is not empty and AR col not empty -> EoL date. AS col not empty -> EoSL date
# <system software><EoL><EoSL>

# Step 2 Match to Archi
# Pull out all system software entities
# For each roadmap entry, match to system software entity - closest match. No of words matched, if number the number of digits matched ( period separator)
# Print out matches or possible matches. 
# Match based on: 
#	Convert roadmap string to text words and versions - split()
#	Match number of words. Do a "in" on archi name.
#   First word of roadmap sw name must be in archi name. 
#   Version must match: word string isnumeric() and match
# Manually create matching table if missing
# Load EoSL and EoL as properties against the system software - import them into Archi
# Load properties and verify / update 

# Prog 2:
# Read in arch objects and relationships
# Find apps related to system software
# For each system software, load in EoL + EoSL properties
# Spit out <app><System software><EoL><EoSL> tuple to csv file
# Enhancement: Edit template and copy for each software line, which apps use it. 


import sys
import os
import uuid
import io
from datetime import datetime

import xlrd
from cmdbconstants import *

roadMapTabSet = {"Workplace", "Data Centre x86-64", "Data Centre Mid Range", "Enterprise Applications", "Enterprise Infra Tools", "Enterprise Security" }
roadMapFile = "RS components - UKT-MAC-7739_Technology_Roadmap_Snapshot_EC_V2.3.xls";

roadMapCol = "EOSL Roadmap"
eolDateCol = "EOL\nDate"
eoslDateCol = "EOSL\nDate"
eolPropName = "EoL Date"
eoslPropName = "EoSL Date"

sysSoftware = dict() #id keyed by systemsoftware name
existingProps = dict() #Keyed by node id + property name
allPropsById = dict() #dict of dict of all properties found for a particular element keyed by its id  and the prop name
props = dict() # New props to add to Archi

roadMapDates = dict() #keyed by tech road map software name value = (eolDate, eoslDate)
nameMapping = dict() # System software name keyed by road map name

unmatched = list()
matches = dict() #Keyed by sysSoftware id, value is (Archi name, Roadmap name, EoL, EoSL)

def readArchiSysSoftware():
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
		if nodeType == "SystemSoftware":
			sysSoftware[lowerName] = id
			nodesById[id] = name
	fnodes.close
	
#Read in existing properties
def readArchiProperties():
	fprops = open("properties.csv")
	count = 0
	lstr = ""
	sysIds = sysSoftware.values()
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
			if id in sysIds:
				#Only add properties if belongs to a system software
				existingProps[propKey] = val
				if id in allPropsById:
					allPropsById[id][name] = val
				else:
					allPropsById[id] = dict()
					allPropsById[id][name] = val
			lstr = ""
	fprops.close

def readNameMapping():
	#Read in name mapping file
	fnodes = open("softwareNameMapping.csv", "r")
	count = 0
	for lstr in fnodes:
		count += 1
		if count == 1: continue
		fs = lstr.rstrip('\n\r').split(",")
		#print fs[0], fs[1], fs[2], fs[3]
		sysName = fs[0].strip().lower()
		if len(fs) > 1:
			roadMapName = fs[1].strip().lower()
			if roadMapName != "":
				nameMapping[sysName] = roadMapName
				#print "%d: %s -> %s" % (len(fs), sysName, roadMapName)
	fnodes.close
	
def convertCellToDate(cell):
	date = None
	if cell.ctype == 1:
		str = cell.value.strip().lower() 
		if str != "n/a" and str != "n\\a" and str != "unknown" and str != "ongoing" and str != "tbd":
			#String of type "Month Year"
			dateStr = cell.value.strip().replace('Sept', 'Sep')
			try:
				date = datetime.strptime(dateStr, "%b %Y")
			except:
				try:
					date = datetime.strptime(dateStr, "%B %Y")
				except Exception as ex:
					try:
						date = datetime.strptime(dateStr, "%d %b %Y")
					except Exception as ex:
						try:
							date = datetime.strptime(dateStr, "%dth %b %Y")
						except Exception as ex:
							try:
								date = datetime.strptime(dateStr, "%B %d, %Y")
							except Exception as ex:
								try:
									date = datetime.strptime(dateStr, "%B %d %Y")
								except Exception as ex:
									print "********************Failed to process date of : %s : %s" % (dateStr, ex)
	elif cell.ctype == 2 or cell.ctype == 3:
		#Excel date
		try:
			date = xlrd.xldate.xldate_as_datetime(cell.value, 0)
		except Exception as ex:
			print "********************Failed to process date of : %s : %s" % (cell.value, ex)
	return date
	
def processRoadMap(cols, row):
	tech = row[cols[roadMapCol]].value.strip()
	eol = row[cols[eolDateCol]]
	eosl = row[cols[eoslDateCol]]
	if tech != "" and (eol.value != "" or eosl.value != ""):
		eolDate = convertCellToDate(eol)
		eoslDate = convertCellToDate(eosl)
		#print "%s Eol: %s EosL: %s" % (tech, eolDate, eoslDate)
		roadMapDates[tech.lower()] = (eolDate, eoslDate)

def isNumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

		##Start
		
print "Processing Roadmap file %s" % roadMapFile
wb = xlrd.open_workbook(roadMapFile)
for sheet in roadMapTabSet:
	#Process each type of software
	ws = None
	try:
		ws = wb.sheet_by_name(sheet)
	except xlrd.XLRDError as x:
		print "Error reading sheet %s" % (sheet)
	if ws is not None:
		count = 0
		for row in ws.get_rows():
			count += 1
			if count == 1:
				rowCsv = rowToCsv(row)
				cols = processHeader(rowCsv)
			else:
				processRoadMap(cols, row)
				
readArchiSysSoftware()
readArchiProperties()
readNameMapping()

#For each system software in archi match to a roadmap entry
for sys in sysSoftware:
	#Check not in mapping file
	sysNew = nameMapping.get(sys, sys)
	#Check not direct match (this will be found in mapping entry)
	tech = roadMapDates.get(sysNew, None)
	matched = False
	if tech == None:
		#Find the version in the system software
		words = sys.split()
		sysVersion = ""
		for word in words:
			if isNumber(word):
				sysVersion = word
			else:
				ws = word.split('v')
				for w in ws:
					if isNumber(w): sysVersion = w

		#print sys, sysVersion
		for tech in roadMapDates:
			#Check how many words match
			#Note: first word must match, as should the version (if present)
			words = tech.split()
			matchCount = 0
			if sysVersion == "": matchVersion = True
			else: matchVersion = False
			if sys.startswith(words[0]): 
				for word in words:
					if isNumber(word) and sysVersion == word: matchVersion = True
					if word in sys: matchCount += 1
			if matchCount > 2 and matchVersion:
				#print "Found match: %s is %s" % (tech, sys)
				date = roadMapDates[tech]
				matches[sysSoftware[sys]] = (sys, tech, date[0], date[1])
				matched = True
				break
	else:
		date = roadMapDates[sysNew]
		matches[sysSoftware[sys]] = (sys, sysNew, date[0], date[1])
		matched = True
		#print "Found exact match: %s is %s" % (sys, sys)
	if not matched:
		unmatched.append(sys)
	
#Add eol properties to matched nodes
for m in matches:
	match = matches[m]
	nodeId = m
	if match[2] != None and (nodeId, eolPropName) not in existingProps:
		props[(nodeId, eolPropName)] = match[2].strftime("%b %Y")
	if match[3] != None and (nodeId, eoslPropName) not in existingProps:
		props[(nodeId, eoslPropName)] = match[3].strftime("%b %Y")

felems = open("new-elements.csv", "w")
print >>felems,'"ID","Type","Name","Documentation"'
felems.close
frels = open("new-relations.csv", "w")
print >>frels,'"ID","Type","Name","Documentation","Source","Target"'
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
	
fprops = open("unmatched.csv", "w")
for m in unmatched:
	#Only add if Software element doesnt already have EoL property 
	eol = props.get((nodeId, eolPropName), 'None')
	eosl = props.get((nodeId, eoslPropName), 'None')
	if (eol == 'None' and eosl == 'None'):
		print >>fprops, '"%s"' % (m)
fprops.close

fprops = open("software-EoL-Dates.csv", "w")
print >>fprops,'"ID","Archi Name","Roadmap Name", "End of support", "End of extended support"'
for match in matches:
	m = matches[match]
	print >>fprops, '"%s","%s","%s","%s","%s"' % (match, m[0], m[1], m[2], m[3])
fprops.close


