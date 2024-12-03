from sshtunnel import SSHTunnelForwarder
import pymysql
import paramiko  # This is needed for SSH key authentication
import pandas as pd
from dotenv import load_dotenv
import json
import os

# load env file
load_dotenv()

# SSH and MySQL connection parameters
ssh_host = os.getenv('ssh_host')  # SSH server
ssh_port = int(os.getenv('ssh_port'))  # SSH port
ssh_user = os.getenv('ssh_user')  # SSH username
private_key_path = os.getenv('private_key_path')  # Path to private key

mysql_host = os.getenv('mysql_host')  # MySQL server host
mysql_port = int(os.getenv('mysql_port'))  # Local port
mysql_user = os.getenv('mysql_user')  # MySQL username
mysql_password = os.getenv('mysql_password')  # MySQL password
mysql_db = os.getenv('mysql_db')  # Database

# Use the private key for SSH authentication
private_key = paramiko.RSAKey.from_private_key_file(private_key_path)

with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=private_key,
        remote_bind_address=(mysql_host, mysql_port)) as tunnel:
    conn = pymysql.connect(host='127.0.0.1', user=mysql_user,
            passwd=mysql_password, db=mysql_db,
            port=tunnel.local_bind_port)
    query = os.getenv('query')
    data = pd.read_sql_query(query, conn)
    print(data)
    conn.close()