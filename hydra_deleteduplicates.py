#!/usr/bin/python3

import datetime
import argparse
import re
import os
import multiprocessing
from collections import OrderedDict
from operator import itemgetter

from hydra import Hydra
from fileinfo import HashFile


class DeleteDuplicates(Hydra):
    def __init__(self, path, no_workers, batch_mode = False, reverse = False):
        self.reverse_order = reverse

        # Init hash function
        self.hash = HashFile()
        self.file_hashes = {}

        # Init hydra stuff - this starts all the workers
        super().__init__(path, no_workers, 'delete_duplicates')

        duplicates = self.main_data

        if len(duplicates) == 0:
            self.logger.info("NO DUPLICATES!")
            return

        # Ask an opinion
        warnings = 0
        self.logger.warning("FOUND " + str(len(duplicates)) + " duplicate files")
        for elem in duplicates:
            # Just a useless check (maybe)
            # NOTE: also check "name (x).xxx" patterns
            if bool(re.search("_[0-9]{1,2}\.[a-zA-Z]+", elem)) is True or \
               bool(re.search("\ \([0-9]+\)\.[a-zA-Z]+", elem)) is True:
                self.logger.warning("DELETE " + elem + " ?")
            else:
                self.logger.warning("DELETE " + elem + " ?-------> WARNING!!!!")
                warnings += 1

        if warnings > 0:
            self.logger.warning("Got " + str(warnings) + " warnings!")

        if batch_mode is False:
            resp = self.get_user_approval("DELETE??!!")
            if resp is False:
                return
        else:
            if warnings > 0:
                self.logger.warning("BATCH MODE - detected warnings, skipping folder " + path)
                return
            else:
                self.logger.warning("BATCH MODE - no warnings, continuing")

        for elem in duplicates:
            self.logger.warning("DELETED file " + elem)
            os.remove(elem)

    def work(self, index, input_file):
        return self.hash.computeHash(input_file)

    def db_insert(self, data):
        self.file_hashes[data['path']] = data['result']

    def db_commit(self):
        no_files = len(self.file_hashes)
        # Sort files, since multiple workers can add them in a different order
        checksum = OrderedDict(sorted(self.file_hashes.items(), key=itemgetter(0), reverse = self.reverse_order))
        duplicates = []

        self.logger.info("FOUND " + str(no_files) + " files. Looking for duplicates")

        # Then look for duplicates - quick and dirty
        for index in range(0, no_files - 1):
            target = list(checksum.keys())[index]
            target_hash = checksum[target]

            # Compare n with [n+1, ... ]
            search = list(checksum.keys())[index + 1:]
            for elem in search:
                if target_hash == checksum[elem] and elem not in duplicates:
                    self.logger.info(target + " and " + elem + " are duplicate!")
                    duplicates.append(elem)

                    # Pass elements to main process, to get use approval
                    self.queue_to_main.put(elem)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete duplicate files in a path")

    parser.add_argument('target', help='Path to index')
    parser.add_argument('--workers', help='Number of workers to spawn', type=int, default=4)
    parser.add_argument('--batch', help='Batch mode. Stops on warnings automatically', action='store_true')
    parser.add_argument('--reverse', help='Reverse ordering. Useful for " (1).xxx" stuff ', action='store_true')
    parser.add_argument('--recursive', help='Run for all files in folder', action='store_true')

    args = parser.parse_args()

    targets = []
    if args.recursive is True:
        for root, dirs, files in os.walk(args.target, topdown=False):
            for dir in dirs:
                path = os.path.join(root, dir)
                targets.append(path)
    else:
        targets = [args.target]

    start = datetime.datetime.now()
    for target in sorted(targets):
        print(target)
        h = DeleteDuplicates(target, args.workers, args.batch, args.reverse)
        del h
    stop = datetime.datetime.now()
    print("This took ", stop - start)
