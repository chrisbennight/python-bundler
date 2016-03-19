import sys
import os
import argparse
import bundle
import unbundle


def validate_paths(target, destination):
    for d in [target, destination]:
        if not os.path.isdir(d):
            print "Directory %s does not exist, exiting" % (d,)
            return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File Packer")
    parser.add_argument('operation_type', help='one of [bundle, unbundle]', choices=['bundle', 'unbundle'])
    parser.add_argument('target', help='directory to bundle, or directory with bundle files to extract')
    parser.add_argument('destination', help='output directory for files')

    if len(sys.argv) < 4:
        print "Missing required parameters"
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    if not validate_paths(args.target, args.destination):
        sys.exit(1)

    if args.operation_type == 'bundle':
        print "Bundling all files and directories under %s to avro bundles in the directory %s" % (
        args.target, args.destination)
        bundle.create_archive(args.target, args.destination)
    elif args.operation_type == 'unbundle':
        print "Unbundling files of the type *.avro in the directory %s to the destination %s" % (
        args.target, args.destination)
        unbundle.extract_archive(args.target, args.destination)
