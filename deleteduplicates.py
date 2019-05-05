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
    def __init__(self, path, no_workers):
        # TODO: maybe there is a better way
        self.queue_to_main = multiprocessing.Queue()

        # Init hash function
        self.hash = HashFile()
        self.file_hashes = {}

        # Init hydra stuff - this starts all the workers
        super().__init__(path, no_workers, 'index_files')

        duplicates = self.queue_to_main.get()

        # Ask an opinion
        print("FOUND", len(duplicates), "duplicate files")
        for elem in duplicates:
            # Just a useless check (maybe)
            if bool(re.search("_[0-9]{1,2}\.[A-Z]+", elem)) is False:
                print(elem, "-------> WARNING!!!!")
            else:
                print(elem)

        #TODO: delete

    def work(self, input_file):
        return self.hash.computeHash(input_file)

    def db_insert(self, data):
        self.file_hashes[data['path']] = data['result']

    def db_commit(self):
        no_files = len(self.file_hashes)
        # Sort files, since multiple workers can add them in a different order
        checksum = OrderedDict(sorted(self.file_hashes.items(), key=itemgetter(0)))
        duplicates = []

        print("FOUND", no_files, "looking for duplicates")

        # Then look for duplicates - quick and dirty
        for index in range(0, no_files - 1):
            target = list(checksum.keys())[index]
            target_hash = checksum[target]

            # Compare n with [n+1, ... ]
            search = list(checksum.keys())[index + 1:]
            for elem in search:
                if target_hash == checksum[elem] and elem not in duplicates:
                    print(target, "and", elem, "are duplicate!")
                    duplicates.append(elem)


        # Need to pass the list to main to get approval from the user
        self.queue_to_main.put(duplicates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete duplicate files in a path")

    parser.add_argument('target', help='Path to index')
    parser.add_argument('--workers', help='Number of workers to spawn',
                        type=int, default=4)

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = DeleteDuplicates(args.target, args.workers)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
