from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class DatabaseConnection():
    def __init__(self, connection_string: str):
        # DEFINE THE ENGINE (CONNECTION OBJECT)
        self.engine = create_engine(connection_string)
        
        # CREATE A SESSION OBJECT TO INITIATE QUERY IN DATABASE
        self.__Session = sessionmaker(bind=self.engine)

    def create_session(self):        
        return self.__Session()
    
    def status(self):
        return self.engine.pool.status()