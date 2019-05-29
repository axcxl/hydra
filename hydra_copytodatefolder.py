#!/usr/bin/python3

import argparse
import datetime

from hydra_movetodatefolder import ToDateFolder

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move to datefolder, based on EXIF DateTimeDigitized or file creation date")

    parser.add_argument('source', help='Where to look for the files to move')
    parser.add_argument('destination', help='Where to move the files. NOTE: it will create subfolders YYYYMMDD in here.')
    parser.add_argument('--workers', help='Number of workers to spawn', type=int, default=4)
    parser.add_argument('--similar', help='Look for similar files in the destination folder', action='store_true')

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = ToDateFolder(args.source, args.destination, args.workers, True, args.similar)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
