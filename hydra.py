import os
import datetime
import ctypes
import time
import hashlib
import queue
import threading
import multiprocessing
import logging
import argparse
from db.files import Files, db_connect, create_deals_table
from sqlalchemy.orm import sessionmaker


class Hydra():
    def __init__(self, path):
        self.target_path = path

        # Init logging
        self.init_logging(logging.INFO, 'hydra.log')

        # Init config stuff
        self.no_workers = 4
        self.hash_func = hashlib.sha256
        self.hash_bsize = 2 * 1024 * 1024 # 8Mb?
        self.pqueue_timeout = 10 #second(s)
        self.pqueue_maxsize = 2048 #files
        self.print_timeout = 1 #second(s)

        # Init db stuff
        self.db_engine = 'sqlite:///files.db'
        self.db_commit_timeout = 5 #seconds

        # Init statistics that come from worker processes
        self.no_files_indexed = multiprocessing.Value(ctypes.c_int, lock=False)
        self.no_files_processed = multiprocessing.Array(ctypes.c_int, self.no_workers, lock=False)
        self.no_files_logged = multiprocessing.Value(ctypes.c_int, lock=False)

        # Init queues
        self.queue_files = multiprocessing.Queue(maxsize=self.pqueue_maxsize)
        self.queue_data = multiprocessing.Queue(maxsize=self.pqueue_maxsize)

        # Init processes
        self.procs = {'walker': multiprocessing.Process(target=self.walk)}
        self.procs['walker'].start()

        for i in range(0, self.no_workers):
            self.procs[str(i)] = multiprocessing.Process(target=self.worker, args=(i,))
            self.procs[str(i)].start()

        self.procs['librarian'] = multiprocessing.Process(target=self.librarian)
        self.procs['librarian'].start()

        # Init threads
        self.timer_db = threading.Timer(self.db_commit_timeout, self.timer_librarian_commit)
        self.timer_db.start()

        # Display statistics
        while True:
            print('Indexed:', self.no_files_indexed.value, ' - PROCESSED: ', end='')
            for i in range(0, self.no_workers):
                print(self.no_files_processed[i], end='; ')
            print('- Logged: ', self.no_files_logged.value, end='')
            print('', end='\r')

            done = True
            for i in range(0, self.no_workers):
                if self.procs[str(i)].is_alive() is True:
                    done = False
                else:
                    self.procs[str(i)].join()
                    self.logger.info('Joined with worker ' + str(i))

            if done is True:
                self.logger.info("Clean-up started!")
                self.timer_db.cancel()
                self.queue_data.close()
                self.queue_files.close()
                self.procs['walker'].join()
                self.procs['librarian'].join()
                print('\n\nALL DONE!')
                self.logger.info('ALL DONE!')
                break

            time.sleep(self.print_timeout)

    def init_logging(self, level, file):
        """
        Moved the logging configuration here to keep the init clean.
        :param level:   logging level to configure
        :param file:    log file to use
        :return: self.logger configured and ready for usage
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
        """
        Look for files on in the given location. Keeps only regular files (an symlinks).
        :return: Nothing. Puts the full path of the files in a queue for processing.
        """
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
        """
        Works on files in the given queue. Puts results in another queue.
        :param index: Used to identify individual workers.
        :return: Nothing. Puts results in another queue.
        """
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

                self.no_files_processed[index] += 1

                fstat = os.stat(target_file)

                data = {"path": target_file,
                        "hash": file_hash.hexdigest(),
                        "size": fstat.st_size,
                        "date": fstat.st_ctime
                        }
                self.queue_data.put(data)
            except FileNotFoundError:
                self.logger.warning('File ' + target_file + ' not found! Maybe symlink?')

        self.logger.info('Worker ' + str(index) + ' finished, processing ' + str(self.no_files_processed[index]) + ' files')

    def librarian(self):
        """
        Logs results to a database. The results are taken from a queue.
        :return:
        """
        self.logger.info('Librarian started!')

        # Connect to the database
        # TODO: improve this, does not look that good
        engine = db_connect(self.db_engine)
        create_deals_table(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        while True:
            try:
                data = self.queue_data.get(timeout=self.pqueue_timeout)
            except queue.Empty:
                break

            if data == 'COMMIT':
                session.commit()
                self.logger.info('COMMIT')
            else:
                self.no_files_logged.value += 1
                session.add(Files(path=data['path'],
                                  hash=data['hash'],
                                  size=data['size'],
                                  date=data['date']))

        self.logger.info('FINAL COMMIT!')
        session.commit()
        self.logger.info('Librarian finished processing ' + str(self.no_files_logged.value) + '!')

    def timer_librarian_commit(self):
        """
        Used to trigger periodic commits. The timer recreates itself until it is canceled at the end.
        :return:
        """
        self.queue_data.put('COMMIT')

        # Restart timer
        self.timer_db = threading.Timer(self.db_commit_timeout, self.timer_librarian_commit)
        self.timer_db.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multiprocess file indexer")

    parser.add_argument('target', help='Path to index')

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = Hydra(args.target)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
