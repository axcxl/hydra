from sqlalchemy import Column, Integer, String
from db import Base


class FilesDb(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    path = Column(String(100))
    hash = Column(String(180))  # TODO: check this if changing hash function
    size = Column(Integer)
    date = Column(String(50))

    def __repr__(self):
        return "<FilesDb(Id=%d, path=%s, hash=%s)>" % (self.id, self.path, self.hash)

"""
WIP
class Photo(FilesDb):
    __tablename__ = 'photo'
    id = Column(Integer, primary_key=True)
    type = Column(String(10))
    camera = Column(String(50))
    orig_date = Column(String(50))
    shutter = Column(String(10))
    fnumber = Column(String(10))
    isorating = Column(String(10))
    flash = Column(String(10))
    focal = Column(String(10))
"""


