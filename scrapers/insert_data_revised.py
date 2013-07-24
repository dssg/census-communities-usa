import pymongo
import os
import datetime
import sys
import json
import csv

MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_CONN = pymongo.MongoClient(MONGO_HOST,27017)
MONGO_DB = MONGO_CONN['census']
MONGO_COLL = MONGO_DB['blocks']

def splitter(filehandler, delimiter=',', row_limit=10000, 
    output_name_template='output_%s.csv', output_path='.', keep_headers=True):
    """
    Splits a CSV file into multiple pieces. Returns a list of paths to pieces.

    Arguments:

        `row_limit`: The number of rows you want in each output file. 10,000 by default.
        `output_name_template`: A %s-style template for the numbered output files.
        `output_path`: Where to stick the output files.
        `keep_headers`: Whether or not to print the headers in each output file.

    """
    reader = csv.reader(filehandler, delimiter=delimiter)
    current_piece = 1
    current_out_path = os.path.join(
         output_path,
         output_name_template  % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'wb'), delimiter=delimiter)
    current_limit = row_limit
    if keep_headers:
        headers = reader.next()
        current_out_writer.writerow(headers)
    outpaths = []
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
               output_path,
               output_name_template  % current_piece
            )
            outpaths.append(current_out_path)
            current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)
    return outpaths

def load_it(path, row_limit):
    split_file = splitter(path, row_limit=row_limit)
    for split in split_file:
        with open(split, 'rb') as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                """ This is where you do the actual conversion/casting """
                row['date'] = datetime.strptime(row.get('createdate'), '%Y-%m-%d')
                """ etc, etc, """
                rows.append(row)
            """ Bulk insert the rows to mongo """
            MONGO_COLL.insert(rows)
  
if __name__ == "__main__":
    if len(sys.argv) < 2:
       print "Please provide the path to the CSV file to load"
       sys.exit()
    else:
      input_file = sys.argv[1]
      load_it(path, 10000) # 10,000 might be a bit ridiculous for 1,000,000,000 rows
