#!/usr/bin/python3

import datetime
import argparse
import threading
import os.path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from hydra import Hydra
from db import Base
from db.filesdb import FilesDb
from fileinfo import HashFile


class IndexFiles(Hydra):
    def __init__(self, path, no_workers):
        # Init hash function
        self.hash = HashFile()

        # Init db stuff
        index = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        self.db_engine = 'sqlite:///' + os.path.join(path, "files_" + index + ".db")
        self.db_commit_timeout = 5  # seconds

        # Connect to the database
        # TODO: improve this, does not look that good
        engine = create_engine(self.db_engine)
        Base.metadata.create_all(engine)
        session_maker = sessionmaker(bind=engine)
        self.session = session_maker()

        # Init autocommit
        self.timer_db = threading.Timer(self.db_commit_timeout, self.timer_librarian_commit)
        self.timer_db.start()

        # Init hydra stuff - this starts all the workers
        super().__init__(path, no_workers, 'index_files')

        # Cleanup
        self.timer_db.cancel()

    def timer_librarian_commit(self):
        """
        Used to trigger periodic commits. The timer recreates itself until it is canceled at the end.
        :return:
        """
        self.queue_data.put('COMMIT')

        # Restart timer
        self.timer_db = threading.Timer(self.db_commit_timeout, self.timer_librarian_commit)
        self.timer_db.start()

    def work(self, input_file):
        return self.hash.computeHash(input_file)

    def db_insert(self, data):
        self.session.add(FilesDb(path=data['path'],
                                 hash=data['result'],
                                 size=data['size'],
                                 date=data['date']))

    def db_commit(self):
        self.session.commit()
        self.logger.info('COMMIT')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multiprocess file indexer")

    parser.add_argument('target', help='Path to index')
    parser.add_argument('--workers', help='Number of workers to spawn',
                        type=int, default=4)

    args = parser.parse_args()

    start = datetime.datetime.now()
    h = IndexFiles(args.target, args.workers)
    stop = datetime.datetime.now()
    print("This took ", stop - start)
