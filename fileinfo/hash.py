import hashlib

class HashFile():
    def __init__(self):
        """
        Prepare parameters for hashing
        """
        self.hash_func = hashlib.sha3_512
        self.hash_bsize = 2 * 1024 * 1024   # 8Mb?

    def computeHash(self, target_file):
        file_hash = self.hash_func()

        with open(target_file, "rb") as f:
            for block in iter(lambda: f.read(self.hash_bsize), b""):
                file_hash.update(block)

        return file_hash.hexdigest()
