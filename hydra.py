import os
import stat
import datetime
import time
import multiprocessing
import logging
import argparse


class Hydra:
    """
    Framework for processing lots of files.
    """
    def __init__(self, path, no_workers, log_name='hydra'):

        self.target_path = path
        self.main_data = []

        # Init logging
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        self.init_logging(logging.DEBUG, log_name + "_" + current_time + ".log")

        # Init config stuff
        self.no_workers = no_workers
        self.pqueue_maxsize = 2048          # files
        self.print_timeout = 1              # second(s)

        # Init statistics that come from worker processes
        self.no_files_indexed = multiprocessing.Value('i', lock=False)
        self.no_files_processed = multiprocessing.Array('i', self.no_workers, lock=False)
        self.no_files_logged = multiprocessing.Value('i', lock=False)

        # Init queues
        self.queue_files = multiprocessing.Queue(maxsize=self.pqueue_maxsize)
        self.queue_data = multiprocessing.Queue(maxsize=self.pqueue_maxsize)
        # TODO: maybe better way?
        self.queue_to_main = multiprocessing.Queue(maxsize=self.pqueue_maxsize)

        self.logger.info("Started working on " + path)

        # Init processes
        self.procs = {'walker': multiprocessing.Process(target=self.walk)}
        self.procs['walker'].start()

        for i in range(0, self.no_workers):
            self.procs[str(i)] = multiprocessing.Process(target=self.worker, args=(i,))
            self.procs[str(i)].start()

        self.procs['librarian'] = multiprocessing.Process(target=self.librarian)
        self.procs['librarian'].start()

        # Display statistics
        while True:
            print('Indexed:', self.no_files_indexed.value, end='')
            print(' - PROCESSED ', end='')
            for i in range(0, self.no_workers):
                print(self.no_files_processed[i], end='; ')
            print('- Logged: ', self.no_files_logged.value, end='')
            print('', end='\r')

            while self.queue_to_main.empty() is False:
                self.main_data.append(self.queue_to_main.get(block=False))

            done = True
            for i in range(0, self.no_workers):
                if self.procs[str(i)].is_alive() is True:
                    done = False
                else:
                    self.procs[str(i)].join()
                    self.logger.debug('Joined with worker ' + str(i))

            if done is True:
                self.logger.debug("Clean-up started!")
                self.queue_data.close()
                self.queue_files.close()
                self.procs['walker'].join()
                break

            time.sleep(self.print_timeout)

        # NOTE: librarian might be still processing data (example: heavy processing in
        # db_commit), so we wait for it separately
        while True:
            # Get all remanaining data
            while self.queue_to_main.empty() is False:
                print(self.queue_to_main.qsize())
                self.main_data.append(self.queue_to_main.get(block=False))

            # See it it is done, non-blocking
            if self.procs['librarian'].is_alive() is False:
                self.procs['librarian'].join()
                break

            time.sleep(1.0)

        self.queue_to_main.close()
        self.logger.debug('ALL DONE!')

    def init_logging(self, level, file):
        """
        Moved the logging configuration here to keep the init clean.
        :param level:   logging level to configure
        :param file:    log file to use
        :return: self.logger configured and ready for usage
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # create a file handler
        handler = logging.FileHandler(os.path.join(self.target_path, file)) # put file in the target folder
        handler.setLevel(level)

        chandler = logging.StreamHandler()
        chandler.setLevel(logging.INFO) # This is fixed for display purposes

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        chandler.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(handler)
        self.logger.addHandler(chandler)

        self.logger.debug('Logging configured')


    def walk(self):
        """
        Look for files on in the given location. Keeps only regular files (an symlinks).
        :return: Nothing. Puts the full path of the files in a queue for processing.
        """
        self.logger.debug('Walker init for ' + self.target_path)

        for root, dirs, files in os.walk(self.target_path):
            for file in sorted(files):
                f = os.path.join(root, file)

                self.logger.debug("FOUND " + f)
                self.no_files_indexed.value += 1
                self.queue_files.put(f)

        self.logger.debug('Processed ' + str(self.no_files_indexed.value) + ' files')

        # Signal that the list of files is done. Do it for each worker
        for i in range(0, self.no_workers):
            self.queue_files.put(None)

    def work(self, input_file):
        """
        Work to do on the file. Can and should be overridden.
        :param input_file: File received from queue.
        :return: should return something
        """
        print("working on ", input_file)
        return input_file

    def worker(self, index):
        """
        Works on files in the given queue. Puts results in another queue.
        The worker terminates when seeing a None in the queue.
        :param index: Used to identify individual workers.
        :return: Nothing. Puts results in another queue.
        """
        self.logger.debug('Worker ' + str(index) + ' started')

        while True:
            target_file = self.queue_files.get()
            if target_file is None:
                break

            try:
                fstat = os.stat(target_file)
                if stat.S_ISREG(fstat.st_mode) is False:
                    continue
            except FileNotFoundError:
                self.logger.warning('File ' + target_file + ' not found! Maybe symlink?')
                continue

            try:
                result = self.work(target_file)

                self.no_files_processed[index] += 1
                self.logger.debug('Work on ' + target_file + ' resulted in ' + str(result))

                data = {"path": target_file,
                        "result": result,
                        "size": fstat.st_size,
                        "date": fstat.st_ctime
                        }
                self.queue_data.put(data)
            except PermissionError:
                self.logger.warning('Permission denied for file ' + target_file)
                continue
            except OSError:
                self.logger.error('ERROR READING FILE ' + target_file)
            except KeyboardInterrupt:
                self.logger.info("STOPPED BY USER")
                break
            except:
                self.logger.error('ERROR FOR FILE' + target_file)
                self.logger.exception('This is the exception')

        self.logger.debug('Worker ' + str(index) + ' finished, processing ' +
                         str(self.no_files_processed[index]) + ' files')

        # Signal to the librarian that this worker is done
        self.queue_data.put(None)


    def db_insert(self, data):
        """
        Insert information in a database. Can and should be overridden.
        NOTE: this is called once per processed file, after worker finished
        :param data: what to insert
        :return:
        """
        print("INSERT", data)

    def db_commit(self):
        """
        Commit database information. Can and should be overridden.
        NOTE: this is called on command (by puttin "COMMIT" in the queue) or ar the end, once.
        :return:
        """
        print("COMMIT!")

    def librarian(self):
        """
        Logs results to a database. The results are taken from a queue.
        The processes terminates after processing all the data and after seeing that all the workers are done. This is
        done by couting the None values in the queue.
        :return:
        """
        self.logger.debug('Librarian started!')

        workers_done = 0

        while True:
            data = self.queue_data.get()
            if data is None:
                workers_done += 1
                # All workers are done, no need to wait anymore
                if workers_done == self.no_workers:
                    break
                continue

            if data == 'COMMIT':
                self.db_commit()
            else:
                self.db_insert(data)
                self.no_files_logged.value += 1

        self.logger.debug('FINAL COMMIT!')
        self.db_commit()
        self.logger.debug('Librarian finished processing ' + str(self.no_files_logged.value) + '!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multiprocess file indexer")

    parser.add_argument('target', help='Path to index')
    parser.add_argument('--workers', help='Number of workers to spawn',
            type=int, default=4)

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = Hydra(args.target, args.workers)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
