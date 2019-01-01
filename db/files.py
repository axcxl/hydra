from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()


def db_connect(db_url):
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(db_url)


def create_deals_table(engine):
    """"""
    DeclarativeBase.metadata.create_all(engine)


class Files(DeclarativeBase):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    path = Column(String(100))
    hash = Column(String(180))  # TODO: check this if changing hash function
    size = Column(Integer)
    date = Column(String(50))

    def __repr__(self):
        return "<Files(Id=%d, path=%s, hash=%s)>" % (self.id, self.path, self.hash)
