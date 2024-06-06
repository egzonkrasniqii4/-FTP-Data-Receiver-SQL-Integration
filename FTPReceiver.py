import os
import logging
import pyodbc
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

# Define the FTP server parameters
FTP_HOST = ''
FTP_PORT = 
FTP_USER = ''
FTP_PASSWORD = ''
FTP_DIRECTORY = r''

# Define SQL Server parameters
SQL_SERVER = ''
SQL_DATABASE = ''
SQL_USERNAME = ''
SQL_PASSWORD = ''

# Set up logging to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def process_and_insert_data(cursor, file, camera_ip, receiver):
    # Process and insert data into SQL Server
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:  # Skip the header line
            fields = line.strip().split(',')
            record_id = int(fields[0])
            timestamp = fields[1]
            period = int(fields[2])
            status = fields[3]
            Polyline0_SUM_IN = int(fields[4])
            Polyline0_SUM_OUT = int(fields[5])

            cursor.execute('''
                INSERT INTO APS180E (record_id, timestamp, period, status, Polyline0_SUM_IN, Polyline0_SUM_OUT, camera_ip, receiver)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (record_id, timestamp, period, status, Polyline0_SUM_IN, Polyline0_SUM_OUT, camera_ip, receiver))

def on_file_received(instance, file):
    logger.info(f"File received: {file}")
    camera_ip = instance.remote_ip  # Get the IP address of the client
    receiver = FTP_USER

    # Connect to SQL Server
    with pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}') as conn:
        cursor = conn.cursor()
        # Process and insert data into SQL Server
        process_and_insert_data(cursor, file, camera_ip, receiver)
        conn.commit()

def create_ftp_server():
    # Ensure the directory exists
    if not os.path.exists(FTP_DIRECTORY):
        os.makedirs(FTP_DIRECTORY)

    # Create a dummy authorizer for managing users
    authorizer = DummyAuthorizer()
    authorizer.add_user(FTP_USER, FTP_PASSWORD, FTP_DIRECTORY, perm='elradfmw')

    # Instantiate FTP handler class
    handler = FTPHandler
    handler.authorizer = authorizer
    handler.banner = "pyftpdlib based FTP server ready."
    handler.on_file_received = on_file_received

    # Instantiate FTP server class
    address = (FTP_HOST, FTP_PORT)
    server = FTPServer(address, handler)

    # Start the FTP server
    logger.info(f"Starting FTP server at {FTP_HOST}:{FTP_PORT}, directory: {FTP_DIRECTORY}")
    server.serve_forever()

if __name__ == "__main__":
    create_ftp_server()
