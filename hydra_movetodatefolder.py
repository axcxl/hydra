#!/usr/bin/python3
# 2019-05-26: tested copy to alternate file name
# 2019-05-26: tested the NO EXIF and the similar stuff

import argparse
import multiprocessing
from collections import OrderedDict
from operator import itemgetter
import datetime
import os
import shutil
import exifread

from hydra import Hydra

class ToDateFolder(Hydra):
    def __init__(self, source, destination, no_workers, copy=False, look_for_similar=False):
        self.source = source
        self.destination = destination

        self.copy = copy  # If true, copy, else MOVE to date folder

        # Init hash function
        self.exif_dates = {}
        self.look_for_similar = look_for_similar
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
            # ...and if we have a list, then the user must choose something
            if type(exifdates[elem]).__name__ == 'list':
                choice = exifdates[elem]
                self.logger.info("------- WARNING!")
                while True:
                    self.logger.info("\tFor " + elem + str(exifdates[elem]))
                    print("\nchoice: Press ENTER for 1, Press 2 for second, enter other date manually")
                    user = input(">")
                    if user == "" or user == "1":
                        exifdates[elem] = choice[0]
                        self.logger.info("User chose 1")
                        break
                    elif user == "2":
                        exifdates[elem] = choice[1]
                        self.logger.info("User chose 2")
                        break
                    else:
                        exifdates[elem] = user # just overwrite directly - not safe, but user!
                        self.logger.info("User input date " + user)
                        break

            # Print elem, either detected or chosen by the user
            self.logger.info(elem + " " + str(exifdates[elem]))

        self.logger.info("Chosen destination " + destination)
        if self.copy is False:
            input("> !! MOVE? !!")
        else:
            input("> COPY?")

        # NOTE: exifdates contains full path filename (fpfile)
        for fpfile in exifdates:
            dest_folder = os.path.join(destination, exifdates[fpfile])
            try:
                # Make sure destination exists
                os.mkdir(dest_folder)
                self.logger.debug("Created " + dest_folder)
            except FileExistsError:
                # Skip if already created
                pass

            file = os.path.basename(fpfile) # extract just the file name
            dest_file = os.path.join(dest_folder, file) # and create the destination full path filename

            # If we already have a file with the same name, append _x to not overwrite
            if os.path.isfile(dest_file) is True:
                index = 1
                self.logger.info(dest_file + " exists, trying other name")
                while os.path.isfile(dest_file) is True:
                    split_name = os.path.splitext(file)
                    dest_file = os.path.join(dest_folder, split_name[0] + "_" + str(index) + split_name[1])
                    index += 1
                self.logger.info("==> came up with " + dest_file)

            if self.copy is False:
                self.logger.info("Moving " + fpfile + " to " + dest_file)
                shutil.move(fpfile, dest_file)
            else:
                self.logger.info("Copying " + fpfile + " to " + dest_file)
                shutil.copy(fpfile, dest_file)

    def work(self, index, input_file):
        tags = exifread.process_file(open(input_file, "rb"), details=False)

        try:
            date = tags["EXIF DateTimeDigitized"].values.split()[0]
            date = str(date)
            date = date.replace(":", "")
            self.last_exif_date = date
        except KeyError:
            ts_epoch = os.path.getmtime(input_file)
            date_mod = datetime.datetime.fromtimestamp(ts_epoch).strftime('%Y%m%d')

            # Look for similar files in the destination folder - useful for lots of duplicates
            if self.look_for_similar is True:
                date_sim = "00000000"
                for rf, df, ff in os.walk(self.destination):
                    if input_file in ff:
                        dest_file = os.path.join(rf, input_file)
                        date_sim = dest_file.split("/")[-2] #TODO: not portable
                        break

                # In this case get user input
                if date_mod == date_sim:
                    date = date_mod
                else:
                    date = [date_mod, date_sim]
            else:
                # DO NOT LOOK FOR SIMILAR FILES, NO EXIF -> try last exif date and last modified time
                # From experience last modified time is the best for this
                if self.last_exif_date is not None:
                    # Give user more choices if possible
                    date = [date_mod, self.last_exif_date]
                else:
                    # No reference date, just use this one
                    date = date_mod

        return date

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
    parser.add_argument('destination', help='Where to move the files. NOTE: it will create subfolders YYYYMMDD in here.')
    parser.add_argument('--workers', help='Number of workers to spawn', type=int, default=4)
    parser.add_argument('--similar', help='Look for similar files in the destination folder', action='store_true')

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = ToDateFolder(args.source, args.destination, args.workers, False, args.similar)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
