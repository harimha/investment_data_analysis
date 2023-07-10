import pandas as pd
from sqlalchemy import create_engine, MetaData
from database.mysql.base.configure import Configuration


class MySQL(Configuration):
    def __init__(self):
        super().__init__()
        self.engine = create_engine(self._url)
        self.metadata = MetaData()

    def commit_statement(self, stmt):
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()



