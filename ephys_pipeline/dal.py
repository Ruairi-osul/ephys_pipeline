from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
import dotenv
import os


class DAL_ORM:
    def __init__(self, engine):
        self.Base = automap_base()
        self.Base.prepare(engine, reflect=True)
        for class_name in self.Base.classes.keys():
            setattr(self, class_name, self.Base.classes[class_name])


class DAL_CORE:
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()
        self.metadata.reflect(bind=engine)
        for table_name in self.metadata.tables.keys():
            setattr(self, table_name, self.metadata.tables[table_name])
