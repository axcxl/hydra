from sqlalchemy import Column, Integer, String
from db import Base


class Files(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    path = Column(String(100))
    hash = Column(String(180))  # TODO: check this if changing hash function
    size = Column(Integer)
    date = Column(String(50))

    def __repr__(self):
        return "<Files(Id=%d, path=%s, hash=%s)>" % (self.id, self.path, self.hash)
