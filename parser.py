import csv, sys, math, operator, re, os, json
from pprint import pprint

try:
	filename = sys.argv[1]
except:
	print "\nPlease input a valid JSON filename.\n"
	print "Format: python scriptname filename.\n"
	exit()

newCsv = []
output = 'dpla.csv'
newFile = open(output, 'wb') #wb for windows, else you'll see newlines added to csv

# initialize csv writer
writer = csv.writer(newFile)

header_row = ('Id', 'IngestType', 'IngestionSequence', 'DataProvider', 'OriginalRecord', 'isShownAt', 'Object', 'Description')

writer.writerow(header_row)

with open(filename) as json_file:
	data = json.load(json_file)
	for s in data:
		source = s['_source']
		try:
			id = source['id']
		except:
			id = ""
		try:
			ingest_type = source['ingestType']
		except:
			ingest_type = ""
		try:
			ingestion_sequence = source['ingestionSequence']
		except:
			ingestion_sequence = ""
		try:
			data_provider = source['dataProvider']
		except:
			data_provider = ""
		try:
			original_record = source['originalRecord']
		except:
			original_record = ""
		try:
			is_shown_at = source['isShownAt']
		except:
			is_shown_at = ""
		try:
			_object = source['object']
		except:
			_object = ""

		try:
			source_resource = source['sourceResource']
		except:
			print "Warning: No source resource in record ID: " + id

		try:
			description = source_resource['description'][0]
			description = description.encode()
		except:
			description = ""

		write_tuple = (id, ingest_type,ingestion_sequence, data_provider, original_record, is_shown_at, _object, description)
		
		writer.writerow(write_tuple)

print "File written to " + output
newFile.close()
