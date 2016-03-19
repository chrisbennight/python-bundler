from avro.datafile import DataFileReader
from avro.io import DatumReader
import os
import hashlib
import sys


def extract_archive(source, destination):
    bundles = dict()

    # arrange files by prefix
    for f in os.listdir(source):
        if f.endswith(".avro"):
            fileparts = f.split("-part-")
            if len(fileparts) == 2:
                prefix = fileparts[0]
                if not bundles.has_key(prefix):
                    bundles[prefix] = []
                bundles[prefix].append(f)

    dr = DatumReader()
    outputfile = None
    try:
        for bundle in bundles.values():
            print "Processing bundle: %s" % (bundle,)
            bundle.sort()
            for f in bundle:
                with open(os.path.join(source, f), 'rb') as avf:
                    reader = DataFileReader(avf, dr)
                    for file_object in reader:
                        if file_object['FSType'] == "DIRECTORY":
                            fullpath = os.path.join(destination, file_object['RelativePath'])
                            if not os.path.exists(fullpath):
                                os.makedirs(fullpath)
                        elif file_object['FSType'] == "FILE":
                            md5 = hashlib.md5()
                            md5.update(file_object['Content'])
                            if md5.hexdigest() != file_object['ContentMD5']:
                                print "File corruption error for file: %s" % (
                                os.path.join(destination, file_object['RelativePath']))
                                sys.exit(1)
                            if file_object['SiblingPartNumber'] == 0:
                                outputfile = open(os.path.join(destination, file_object['RelativePath']), 'wb')
                            if outputfile.closed:
                                print "closed"
                            outputfile.write(file_object['Content'])
                            if file_object['NumberSiblings'] == file_object['SiblingPartNumber']:
                                outputfile.close()
    finally:
        if outputfile is not None:
            if not outputfile.closed:
                outputfile.close()
