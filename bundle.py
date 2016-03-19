import avro.schema
from avro.datafile import DataFileWriter
import os
import time
import avro.io
import hashlib
import math

maxfilesize = 800 * 1000 * 1000


def makedir(name, relative_path):
    return {
        "FSType": "DIRECTORY",
        "Name": name,
        "RelativePath": relative_path,
        "NumberSiblings": 0,
        "SiblingPartNumber": 0,
        "ContentMD5": None,
        "Content": None
    }


def makefile(name, relative_path, number_siblings, sibling_part_number, bytes):
    md5 = hashlib.md5()
    md5.update(bytes)
    return {
        "FSType": "FILE",
        "Name": name,
        "RelativePath": relative_path,
        "NumberSiblings": number_siblings,
        "SiblingPartNumber": sibling_part_number,
        "ContentMD5": md5.hexdigest(),
        "Content": bytes
    }


def get_file_chunks(file):
    size = os.path.getsize(file)
    numsiblings = int(math.ceil(float(size) / float(maxfilesize))) - 1
    sibling_number = 0
    with open(file, 'rb') as fd:
        while True:
            data = fd.read(maxfilesize)
            if not data:
                break
            yield sibling_number, numsiblings, data
            sibling_number += 1


def rotate_avro_file(fd, writer, iteration, fileprefix, destdir, datum, schema):
    iteration += 1
    avrofile = fileprefix + "-part-{0:04d}.avro".format(iteration)
    writer.close()
    fd.close()
    fd = open(os.path.join(destdir, avrofile), 'wb')
    writer = DataFileWriter(fd, datum, schema,codec='deflate')
    return fd, writer, iteration


def create_archive(basedir, destdir):
    all_files = []
    all_dirs = []

    # make a snapshot in case the output directory is the bundle source - so we don't recursively bundle the output
    for path, dirs, files in os.walk(basedir):
        for d in dirs:
            dir = os.path.join(path, d)
            all_dirs.append(dir)
        for f in files:
            file = os.path.join(path, f)
            all_files.append(file)

    schema = avro.schema.parse(open("avro-schemas.json").read())
    fileprefix = time.strftime("%Y%m%d-%H%M%S")
    avrofile = fileprefix + "-part-0001.avro"
    iteration = 1

    fd = open(os.path.join(destdir, avrofile), 'wb')
    datum = avro.io.DatumWriter()
    writer = DataFileWriter(fd, datum, schema, codec='deflate')
    try:
        for d in all_dirs:
            val = makedir(os.path.basename(os.path.normpath(d)),
                          os.path.relpath(d, basedir))
            writer.append(val)

        for f in all_files:
            for sibling, numsiblings, chunk in get_file_chunks(f):
                if (fd.tell() + len(chunk)) > maxfilesize * 1.1:
                   fd, writer, iteration = rotate_avro_file(fd,
                                                             writer,
                                                             iteration,
                                                             fileprefix,
                                                             destdir,
                                                             datum,
                                                             schema)
                file = makefile(os.path.basename(os.path.normpath(f)),
                                os.path.relpath(f, basedir),
                                numsiblings,
                                sibling,
                                chunk)
                writer.append(file)
    finally:
        writer.close()
        fd.close()





