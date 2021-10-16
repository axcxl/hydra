#!/usr/bin/python3
import argparse
import datetime
import os
import sqlite3

from hydra import Hydra


class CompareDb(Hydra):
    def __init__(self, sourcedb, targetdb, no_workers):
        self.sourcedb = sourcedb
        self.targetdb = targetdb

        # Store the db in memory for each worker
        self.mem_dbs = list(range(no_workers))

        # Init hydra stuff - this starts all the workers
        super().__init__(os.getcwd(), no_workers, 'compare_dbs')

    def walk(self):
        """
        Override the walk function to walk the source database
        :return:
        """
        self.logger.debug('Walker init for ' + self.sourcedb)
        source_conn = sqlite3.connect(self.sourcedb)

        source_curs = source_conn.cursor()
        source_curs.execute('SELECT * FROM {}'.format("files"))

        while True:
            elem = source_curs.fetchone()
            if elem is None:
                break

            self.no_elems_indexed.value += 1
            self.queue_elems.put(elem) # Put entire element - easier to print stuff

        self.logger.debug('Processed ' + str(self.no_elems_indexed.value) + ' files')
        self.worker_signal_done()

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

    def work(self, index, input_data):
        filename = input_data[1]
        hash = input_data[2]
        self.mem_dbs[index].execute('SELECT path FROM files WHERE hash=?', (hash,))

        found = self.mem_dbs[index].fetchone()
        self.logger.debug("For "+ filename + "(" + hash + ") found " + str(found))
        if found is not None:
            return True
        else:
            return False

    def db_insert(self, data):
        if data['result'] is not True:
            file = data['path'][1]
            self.logger.warning(file + " not found in " + self.targetdb)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multiprocess file indexer")

    parser.add_argument('source', help='Path to database 1')
    parser.add_argument('target', help='Path to database 2')
    parser.add_argument('--workers', help='Number of workers to spawn',
                        type=int, default=4)

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = CompareDb(args.source, args.target, args.workers)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
