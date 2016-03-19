# python-bundler
a poor knockoff of tar, using avro as the storage format

I needed a way to package directories of file that wasn't tar or zip - and also integrated some split functionality (no files over a certain size)

# Usage

```
$ python filepacker.py --help
Missing required parameters
usage: filepacker.py [-h] {bundle,unbundle} target destination

File Packer

positional arguments:
  {bundle,unbundle}  one of [bundle, unbundle]
  target             directory to bundle, or directory with bundle files to
                     extract
  destination        output directory for files

optional arguments:
  -h, --help         show this help message and exit
```

default max filesize is 800MB
