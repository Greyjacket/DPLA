import csv, sys, math, operator, re, os, json, ijson
from pprint import pprint

filelist = []

for file in os.listdir("."):
    if file.endswith(".json"):
        filelist.append(file)

for input in filelist:

	newCsv = []
	splitlist = input.split(".")
	output = splitlist[0] + '.csv'

	newFile = open(output, 'wb') #wb for windows, else you'll see newlines added to csv

	# initialize csv writer
	writer = csv.writer(newFile)

	header_row = ('Id', 'IngestType', 'IngestionSequence', 'DataProvider', 
		'OriginalRecord', 'isShownAt', 'Object', 'Description', 'Format', 'Identifier', 'LanguageName', 'LanguageISO',
		'Rights', 'Title', 'Collection','Type', 'Date', 'Spatial', 'Subject')

	writer.writerow(header_row)

	with open(input) as json_file:
		#data = json.load(json_file)
		data = ijson.items(json_file, 'item')
		# https://stackoverflow.com/questions/37898065/memory-issues-while-parsing-json-file-in-ijson
		for s in data:
			source = s['_source']

			try:
				id = source['id'].encode()
			except:
				id = ""
			try:
				ingest_type = source['ingestType'].encode()
			except:
				ingest_type = ""
			try:
				ingestion_sequence = source['ingestionSequence'].encode()
			except:
				ingestion_sequence = ""
			try:
				data_provider = source['dataProvider'].encode()
			except:
				data_provider = ""
			try:
				original_record = source['originalRecord'].encode()
			except:
				original_record = ""
			try:
				is_shown_at = source['isShownAt'].encode()
			except:
				is_shown_at = ""
			try:
				_object = source['object'].encode()
			except:
				_object = ""

			try:
				source_resource = source['sourceResource']
			except:
				print "Warning: No source resource in record ID: " + id

			try:
				descriptions = source_resource['description']
				string = ""
				for item in descriptions:
					if len(descriptions) > 1:
						description = item.encode() + " | "
					else:
						description = item.encode()
					string = string + description
				description = string.encode()
			except:
				description = ""

			try:
				formats = source_resource['format']
				string = ""
				for item in formats:
					if len(formats) > 1:
						format = item.encode() + " | "
					else:
						format = item.encode()
					string = string + format
				format = string.encode()
			except:
				format = ""

			try:
				identifiers = source_resource['identifier']
				string = ""
				for item in identifiers:
					if len(identifiers) > 1:
						identifier = item.encode() + " | "
					else:
						identifier = item.encode()
					string = string + identifier
				identifier = string.encode()
			except:
				identifier = ""

			try:
				language = source_resource['language'][0]
				try:
					language_name = language['name'].encode()
				except:
					language_name = ""
				try:
					language_iso = language['iso639_3'].encode()
				except:
					language_iso = ""
			except:
				language_name = ""
				language_iso = ""

			try:
				rights = source_resource['rights'].encode()
			except:
				rights = ""

			try:
				titles = source_resource['title']
				string = ""
				for item in titles:
					if len(titles) > 1:
						title = item.encode() + " | "
					else:
						title = item.encode()
					string = string + title
				title = string.encode()
			except:
				title = ""

			try:
				collection = source_resource['collection']
				string = ""

				for item in collection:
					try:
						collection_title = item['title'][0]
						id = item['id']
					except:
						collection_title = ""
						id = ""
					string = string + "Title: " + collection_title + ", Id: " + id
							
					if len(collection) > 1:
						string = string + " | "

				collection = string.encode()
			except:
				collection = ""

			try:
				type = source_resource['type']
			except:
				type = ""

			try:
				dates = source_resource['date']
				string = ""

				for item in dates:
					try:
						display_date = item['displayDate']
						begin = item['begin']
						end = item['end']
					except:
						display_date = ""
						begin = ""
						end = ""
					string = string + "Display Date: " + display_date + ", Begin: " + begin + ", End: " + end
							
					if len(dates) > 1:
						string = string + " | "
				date = string.encode()
			except:
				date = ""

			try:
				spatial = source_resource['spatial']
				string = ""

				for item in spatial:
					try:
						name = item['name']
					except:
						name = ""
					try:
						coordinates = item['coordinates']
					except:
						coordinates = ""
					try:
						city = item['city']
					except:
						city = ""
					try:
						county = item['county']
					except:
						county = ""
					try:
						country = item['country']
					except:
						country = ""
					try:
						region = item['region']
					except:
						region = ""
					try:
						state = item['state']
					except:
						state = ""

					string = string + "Name: " + name + ", Coordinates: " + coordinates + ", City: " + city + ", County: " + county + ", Country: " + country + ", Region: " + region + ", State: " + state
					
					if len(spatial) > 1:
						string = string + " | "

				spatial = string.encode()
			except:
				spatial = ""

			try:
				subjects = source_resource['subject']
				string = ""

				for item in subjects:
					try:
						name = item['name']
					except:
						name = ""

					string = string + "Name: " + name 
					
					if len(subjects) > 1:
						string = string + " | "

				subject = string.encode()
			except:
				subject = ""

			write_tuple = (id, ingest_type,ingestion_sequence, data_provider, original_record, 
				is_shown_at, _object, description, format, identifier, language_name, language_iso, 
				rights, title, collection, type, date, spatial, subject)
			
			writer.writerow(write_tuple)

	print "File written to " + output
	newFile.close()
