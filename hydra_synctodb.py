#!/usr/bin/python3
import shutil
import datetime
import stat
import argparse
import os.path
import sqlite3
import logging
import multiprocessing

from hydra import Hydra
from fileinfo import HashFile
from utils.pathsplitall import pathsplitall


class SyncToDb(Hydra):
    def __init__(self, path, targetdb, strippath, no_workers, dry_run=False):
        # Init hash function
        self.hash = HashFile()

        # Init db stuff
        self.targetdb = targetdb
        self.strippath = strippath

        # Store the db in memory for each worker
        self.mem_dbs = list(range(no_workers))

        self.dry_run = dry_run
        self.files_skipped = multiprocessing.Value('i', lock=False)
        self.files_moved = multiprocessing.Value('i', lock=False)

        # Init hydra stuff - this starts all the workers
        super().__init__(path, no_workers, 'sync_to_db', log_level=logging.DEBUG)

    def init(self, index):
        """
        For each worker, create a memory db and save cursors
        :param index:
        :return:
        """
        self.logger.debug("Creating mem db for worker " + str(index))
        target_conn = sqlite3.connect(self.targetdb)
        tmp = sqlite3.connect(':memory:')
        self.mem_dbs[index] = tmp.cursor()
        target_conn.backup(tmp)
        target_conn.close()
        self.logger.debug("Done creating mem db for worker " + str(index))

    def work(self, index, input_file):
        fstat = os.stat(input_file)
        if stat.S_ISREG(fstat.st_mode) is False:
            return None

        # Get file info
        file_hash = self.hash.computeHash(input_file)
        file_size = fstat.st_size
        file_time = fstat.st_ctime

        file_name = os.path.basename(input_file)

        # Look file up in database
        self.mem_dbs[index].execute('SELECT path,size,date FROM files WHERE hash=?', (file_hash,))

        while True:
            found = self.mem_dbs[index].fetchone()
            if found is None:
                break
            if os.path.basename(found[0]) == file_name:
                break

        self.logger.debug("For "+ input_file+ "(" + file_hash + ") found " + str(found))

        # No file found in targetdb -> ignore
        if found is None:
            return False

        # Some sanity checks
        if file_size != found[1]:
            self.logger.warning("SIZE MISMATCH FOR " + input_file + "! Skipping")
            return None
        # TOO MANY FAILURES
        #if file_time != found[2]:
        #    self.logger.warning("DATE MISMATCH FOR " + input_file + "! Skipping! (GOT:" + str(file_time) + " FILE:" + str(found[2]) + ")")
        #    return None
        initial_path = pathsplitall(found[0])
        split = initial_path.index(self.strippath)
        target_path = initial_path[split+1:]
        target_path = os.path.join(*target_path)

        return target_path

    def db_insert(self, data):
        # Skip files not found
        if data['result'] is False:
            self.files_skipped.value += 1
            return

        target_folder = os.path.dirname(data['result'])
        try:
            if self.dry_run is False:
                os.mkdir(target_folder)
            self.logger.warning("Created folder " + target_folder)
        except FileExistsError:
            #Skip if already created
            pass

        if self.dry_run is False:
            shutil.copy(data['path'], data['result'])
        self.logger.warning("Moving " + data['path'] + " to " + data['result'])
        self.files_moved.value += 1

    def db_commit(self):
        self.logger.warning("FINAL STATS: " + str(self.files_moved.value) + "moved / " + \
                            str(self.files_skipped.value) + " skipped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move files to the paths given in a db")

    parser.add_argument('target', help='Path to index')
    parser.add_argument('targetdb', help='Database to use as reference')
    parser.add_argument('strippath', help="From where to cut the path in the target db")
    parser.add_argument('--dryrun', help='Do not actually move files or create folders', action='store_true')
    parser.add_argument('--workers', help='Number of workers to spawn',
                        type=int, default=4)

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = SyncToDb(args.target, args.targetdb, args.strippath, args.workers, args.dryrun)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
