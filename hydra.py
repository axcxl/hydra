import os
import ctypes
import time
import hashlib
import queue
import multiprocessing
import logging
import argparse

class Hydra():
    def __init__(self, path):
        self.target_path = path

        # Init logging
        self.init_logging(logging.INFO, 'hydra.log')

        # Init config stuff
        self.no_workers = 2
        self.hash_func = hashlib.sha3_256
        self.hash_bsize = 2 * 1024 * 1024 # 2Mb?
        self.pqueue_timeout = 10 #second(s)
        self.pqueue_maxsize = 2048 #files
        self.print_timeout = 1 #second(s)

        # Init statistics that come from worker processes
        self.no_files_indexed = multiprocessing.Value(ctypes.c_int, lock=False)
        self.no_files_processed = multiprocessing.Array(ctypes.c_int, self.no_workers, lock=False)

        # Init queues
        self.queue_files = multiprocessing.Queue(maxsize=self.pqueue_maxsize)

        # Init processes
        self.procs = {'walker': multiprocessing.Process(target=self.walk)}
        self.procs['walker'].start()

        for i in range(1,1 + self.no_workers):
            self.procs[str(i)] = multiprocessing.Process(target=self.worker, args=(i,))
            self.procs[str(i)].start()

        while True:
            print('Indexed:', self.no_files_indexed.value, ' - PROCESSED: ', end='')
            for i in range(0, self.no_workers):
                print(self.no_files_processed[i], end='; ')
            print('', end=' ' * 80 + '\r')

            done = True
            for elem in self.procs:
                if self.procs[elem].is_alive() is True:
                    done = False
                else:
                    self.procs[elem].join()

            if done is True:
                print('\n\nALL DONE!')
                exit(0)

            time.sleep(0.1)

    def init_logging(self, level, file):
        """
        Moved the logging configuration here to keep the init clean.
        :param level:   logging level to configure
        :param file:    log file to use
        :return:
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # create a file handler
        handler = logging.FileHandler(file)
        handler.setLevel(level)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(handler)

        self.logger.info('Logging configured')

    def walk(self):
        self.logger.info('Walker init for ' + self.target_path)

        for root, dirs, files in os.walk(self.target_path):
            for file in files:
                f = os.path.join(root, file)

                # Keep only regular files and symlinks
                if os.path.isfile(f) is not True:
                    continue

                self.logger.debug("FOUND " + f)
                self.no_files_indexed.value += 1
                self.queue_files.put(f)

        self.logger.info('Processed ' + str(self.no_files_indexed.value) + ' files')

    def worker(self, index):
        self.logger.info('Worker ' + str(index) + ' started with ' + str(self.hash_func))

        file_hash = self.hash_func()
        while True:
            try:
                target_file = self.queue_files.get(timeout=self.pqueue_timeout)
            except queue.Empty:
                break

            self.logger.debug("Processing file " + target_file)

            try:
                with open(target_file, "rb") as f:
                    for block in iter(lambda: f.read(self.hash_bsize), b""):
                        file_hash.update(block)

                self.no_files_processed[index-1] += 1
                self.logger.debug("Computed HASH %s for file %s" % (file_hash.hexdigest(), target_file))
            except FileNotFoundError:
                self.logger.warning('File ' + target_file + ' not found! Maybe symlink?')

        self.logger.info('Worker ' + str(index) + ' finished, processing ' + str(self.no_files_processed[index-1]) + ' files')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multiprocess file indexer")

    parser.add_argument('target', help='Path to index')

    args = parser.parse_args()

    h = Hydra(args.target)
