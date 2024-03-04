import subprocess
import time

def start_pg(host, max_retries=6, delay_seconds=6):
    retries = 0
    while retries < max_retries:
        try:
            subprocess.run(["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            print("Successfully connected to PostgreSQL!")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Damn Error connecting to PostgreSQL: {e}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False

# Use the function before running the ELT process
if not start_pg(host="pgdatabase"):
    exit(1)

print("Starting ELT script...")

# Configuration for the source PostgreSQL database
source_config = {
    'dbname': 'ny_taxi_0',
    'user': 'root',
    'password': 'root',
    'host': 'pgdatabase'
}

# Configuration for the destination PostgreSQL database
destination_config = {
    'dbname': 'taxidatawh_db',
    'user': 'root',
    'password': 'root',
    'host': 'taxidatawh'
}

# Use pg_dump to dump the source database to a SQL file
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'datadump.sql',
    '-w'  # Do not prompt for password
]

# Set the PGPASSWORD environment variable to avoid password prompt
subprocess_env = {'PGPASSWORD': source_config['password']}

# Execute the dump command
subprocess.run(dump_command, env=subprocess_env, check=True)

# Use psql to load the dumped SQL file into the destination database
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'datadump.sql'
]

# Set the PGPASSWORD environment variable for the destination database
subprocess_env = {'PGPASSWORD': destination_config['password']}

# Execute the load command
subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT script...")