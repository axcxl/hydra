#!/usr/bin/python3

import sys

import sqlite3


class CompareDatabases:
    def __init__(self, sourcedb, targetdb):
        self.source = sqlite3.connect(sourcedb)
        self.target = sqlite3.connect(targetdb)

        self.dbDifferences()

        self.source.close()
        self.target.close()

    def dbDifferences(self):
        sc = self.source.cursor()
        sc.execute('SELECT * FROM {}'.format("files"))

        while True:
            elem = sc.fetchone()
            if elem is None:
                break

            hash = elem[2]
            tg = self.target.cursor()
            tg.execute('SELECT path FROM files WHERE hash=?', (hash,))
            found = tg.fetchall()
            print(found)
            input("next")


if __name__ == "__main__":
    cd = CompareDatabases(sys.argv[1], sys.argv[2])
