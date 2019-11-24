#!/usr/bin/python3

import argparse
import multiprocessing
from collections import OrderedDict
from operator import itemgetter
import datetime
import os
import shutil
import exifread

from hydra import Hydra

class RenameToTime(Hydra):
    def __init__(self, source, no_workers):
        self.source = source

        # Init hash function
        self.exif_dates = {}
        self.last_exif_date = None

        # Init hydra stuff - this starts all the workers
        super().__init__(source, no_workers, 'move_to_date_folder')

        # Get results from librarian
        exifdates = OrderedDict()
        for elem in self.main_data:
            exifdates[elem[0]] = elem[1]

        # Exit if nothing to do
        if len(exifdates) == 0:
            self.logger.info("NO FILES FOUND!")
            exit(0)

        # Look through the results...
        for elem in exifdates:
            if "000000" in exifdates[elem]:
                self.logger.warning("Warning for " + elem + " no date found! Skipping!")
            else:
                self.logger.info("Renaming " + elem + " to " + exifdates[elem])

        input("> RENAME?")

        # NOTE: exifdates contains full path filename (fpfile)
        for fpfile in exifdates:
            if "000000" not in exifdates[fpfile]:
                dest_folder = os.path.dirname(fpfile)
                dest_file = os.path.join(dest_folder, exifdates[fpfile])

                print("renaming", fpfile, "to", dest_file)

                #shutil.move(fpfile, dest_file)

    def work(self, index, input_file):
        tags = exifread.process_file(open(input_file, "rb"), details=False)
        ext = os.path.basename(input_file).split(".")[1]

        try:
            date = tags["EXIF DateTimeDigitized"].values.split()[1]
            date = str(date)
            date = date.replace(":", "")
            self.last_exif_date = date
        except KeyError:
            date = "000000"

        return date + "." + ext

    def db_insert(self, data):
        # This is called in same process as commit, so we can create the list to pass on to main
        self.exif_dates[data['path']] = data['result']

    def db_commit(self):
        # Sort files, since multiple workers can add them in a different order
        exifdates = OrderedDict(sorted(self.exif_dates.items(), key=itemgetter(0)))

        # Need to pass the list to main to get approval from the user
        for elem in exifdates:
            self.queue_to_main.put([elem, exifdates[elem]])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move to datefolder, based on EXIF DateTimeDigitized or file creation date")

    parser.add_argument('source', help='Where to look for the files to move')
    parser.add_argument('--workers', help='Number of workers to spawn', type=int, default=4)

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = RenameToTime(args.source, args.workers)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
