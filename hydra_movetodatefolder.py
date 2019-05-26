#!/usr/bin/python3
# 2019-05-26: tested copy to alternate file name
# TODO: test the NO EXIF and the similar stuff

import datetime
import argparse
import multiprocessing
from collections import OrderedDict
from operator import itemgetter
import os
import shutil
import exifread

from hydra import Hydra

class MoveToDateFolder(Hydra):
    def __init__(self, source, destination, no_workers, look_for_similar=False):
        # TODO: maybe there is a better way
        self.queue_to_main = multiprocessing.Queue()

        # Init hash function
        self.exif_dates = {}
        self.look_for_similar = look_for_similar
        self.last_exif_date = None

        # Init hydra stuff - this starts all the workers
        super().__init__(source, no_workers, 'move_to_date_folder')

        exifdates = self.queue_to_main.get()

        for elem in exifdates:
            if type(exifdates[elem]).__name__ == 'list':
                choice = exifdates[elem]
                # The user has to make a choice
                print("------- WARNING!")
                while True:
                    print("\nchoice: Press ENTER for 1, Press 2 for second")
                    user = input(">")
                    if user == "" or user == "1":
                        exifdates[elem] = choice[0]
                        break
                    elif user == "2":
                        exifdates[elem] = choice[1]
                        break

            print(elem, exifdates[elem])

        print("Moving files to desination", destination)
        input("> MOVE?")

        for fpfile in exifdates:
            dest_folder = os.path.join(destination, exifdates[fpfile])
            try:
                os.mkdir(dest_folder)
            except FileExistsError:
                pass

            file = os.path.basename(fpfile)
            dest_file = os.path.join(dest_folder, file)
            if os.path.isfile(dest_file) is True:
                index = 1
                print(dest_file, "exists, trying other name")
                while os.path.isfile(dest_file) is True:
                    split_name = os.path.splitext(file)
                    dest_file = os.path.join(dest_folder, split_name[0] + "_" + str(index) + split_name[1])
                    index += 1
                print("came up with", dest_file)

            print("Moving", fpfile, "to", dest_file)
            shutil.copy(fpfile, dest_file)

    def work(self, input_file):
        tags = exifread.process_file(open(input_file, "rb"), details=False)

        try:
            date = tags["EXIF DateTimeDigitized"].values.split()[0]
            date = str(date)
            date = date.replace(":", "")
            self.last_exif_date = date
        except KeyError:
            ts_epoch = os.path.getmtime(input_file)
            date_mod = datetime.fromtimestamp(ts_epoch).strftime('%Y%m%d')

            # Look for similar files in the destination folder - useful for lots of duplicates
            if self.look_for_similar is True:
                for rf, df, ff in os.walk(args.dest):
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
        self.exif_dates[data['path']] = data['result']

    def db_commit(self):
        no_files = len(self.exif_dates)
        # Sort files, since multiple workers can add them in a different order
        exifdates = OrderedDict(sorted(self.exif_dates.items(), key=itemgetter(0)))

        # Need to pass the list to main to get approval from the user
        self.queue_to_main.put(exifdates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move to datefolder, based on EXIF DateTimeDigitized or file creation date")

    parser.add_argument('source', help='Where to look for the files to move')
    parser.add_argument('destination', help='Where to move the files. NOTE: it will create subfolders YYYYMMDD in here.')
    parser.add_argument('--workers', help='Number of workers to spawn', type=int, default=4)
    parser.add_argument('--similar', help='Look for similar files in the destination folder', action='store_true')

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = MoveToDateFolder(args.source, args.destination, args.workers, args.similar)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
