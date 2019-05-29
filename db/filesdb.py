from sqlalchemy import Column, Integer, String
from db import Base


class FilesDb(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    path = Column(String(100))
    hash = Column(String(180))  # TODO: check this if changing fileinfo function
    size = Column(Integer)
    date = Column(String(50))

    #TODO: check length of these
    camera = Column(String(100))
    lens = Column(String(100))
    exp_time = Column(String(100))
    exp_fnum = Column(String(100))
    exp_iso  = Column(String(100))
    focal_length = Column(String(100))
    flash = Column(String(100))


    def __repr__(self):
        return "<FilesDb(Id=%d, path=%s, fileinfo=%s)>" % (self.id, self.path, self.hash)
