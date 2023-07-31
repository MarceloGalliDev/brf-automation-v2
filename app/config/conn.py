#carregamento e importações
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
import os
import logging


class DatabaseConnection:
    def __init__(self):
        load_dotenv()
        self.LOG_DIRECTORY = os.getenv('LOG_DIRECTORY')
        self.LOG_FILE = os.path.join(self.LOG_DIRECTORY, 'data.log')
        self.db_url = os.getenv('URL')

    def initialize_logging(self):
        if not os.path.exists(self.LOG_DIRECTORY):
            os.makedirs(self.LOG_DIRECTORY)
        logging.basicConfig(
            filename=self.LOG_FILE,
            level=logging.INFO,
            format='%(asctime)s %(message)s',
            datefmt='%d/%m/%Y %I:%M:%S %p -',
            encoding='utf-8'
        )


    def log_data(self):        
        for arquivo in os.listdir(self.LOG_DIRECTORY):
            if arquivo.endswith('.log'):
                logging.info('Arquivo iniciado')

        
    def get_db_engine(self):
        try:
            self.db_url = os.getenv('URL')
            engine = create_engine(self.db_url)
            # Test connection
            with engine.connect() as connection:
                logging.info('Conexão estabelecida!')
                pass
            logging.info('Banco de dados conectado!')
            return engine
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            return None


if __name__ == "__main__":
    db_connection = DatabaseConnection()    
    db_connection.log_data()
    db_connection.get_db_engine()