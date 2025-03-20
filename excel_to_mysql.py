import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import configparser
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename=f"excel_to_mysql_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def read_config(config_file='config.ini'):
    """Read database configuration from config file"""
    if not os.path.exists(config_file):
        create_default_config(config_file)
        logging.info(f"Created default config file: {config_file}")
        print(f"Created default config file: {config_file}")
        print("Please update the config file with your database credentials and try again.")
        exit(1)
        
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'host': config['DATABASE']['host'],
        'database': config['DATABASE']['database'],
        'user': config['DATABASE']['user'],
        'password': config['DATABASE']['password'],
        'table': config['DATABASE']['table'],
        'excel_file': config['FILES']['excel_file']
    }

def create_default_config(config_file):
    """Create a default configuration file"""
    config = configparser.ConfigParser()
    config['DATABASE'] = {
        'host': 'localhost',
        'database': 'your_database',
        'user': 'your_username',
        'password': 'your_password',
        'table': 'form_responses'
    }
    config['FILES'] = {
        'excel_file': 'google_form_responses.xlsx'
    }
    
    with open(config_file, 'w') as f:
        config.write(f)

def connect_to_database(config):
    """Connect to MySQL database"""
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        if connection.is_connected():
            logging.info("Connected to MySQL database")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL database: {e}")
        print(f"Error connecting to MySQL database: {e}")
        return None

def create_table_if_not_exists(connection, excel_file, table_name):
    """Create table based on Excel file columns if it doesn't exist"""
    try:
        # Read Excel file to get column names
        df = pd.read_excel(excel_file)
        
        # Print DataFrame info for debugging
        logging.info(f"DataFrame info: {df.info()}")
        
        # Clean column names for MySQL
        columns = [f"`{col.replace(' ', '_').replace('-', '_').replace('.', '_')}`" for col in df.columns]
        
        # Create SQL for table creation
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, "
        create_table_sql += ", ".join([f"{col} TEXT" for col in columns])
        create_table_sql += ")"
        
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        connection.commit()
        logging.info(f"Table {table_name} created or already exists")
        
        return df, columns
    except Error as e:
        logging.error(f"Error creating table: {e}")
        print(f"Error creating table: {e}")
        return None, None

def convert_timestamp(val):
    """Convert timestamp values to string format that MySQL can handle"""
    if pd.isna(val):
        return None
    elif isinstance(val, pd.Timestamp):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return str(val)

def insert_data(connection, df, columns, table_name):
    """Insert data from DataFrame into MySQL table"""
    try:
        cursor = connection.cursor()
        
        # Prepare SQL for insert
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Insert each row with proper type conversion
        records = 0
        for _, row in df.iterrows():
            # Convert each value to a format MySQL can handle
            values = [convert_timestamp(val) for val in row]
            cursor.execute(insert_sql, tuple(values))
            records += 1
            
        connection.commit()
        logging.info(f"Successfully inserted {records} records into {table_name}")
        print(f"Successfully inserted {records} records into {table_name}")
        
        return True
    except Error as e:
        logging.error(f"Error inserting data: {e}")
        print(f"Error inserting data: {e}")
        return False

def main():
    """Main function to transfer Excel data to MySQL"""
    print("Starting Excel to MySQL data transfer...")
    logging.info("Starting Excel to MySQL data transfer")
    
    # Read configuration
    config = read_config()
    
    # Connect to database
    connection = connect_to_database(config)
    if not connection:
        return
    
    # Create table if not exists and get DataFrame
    df, columns = create_table_if_not_exists(connection, config['excel_file'], config['table'])
    if df is None:
        connection.close()
        return
    
    # Log data types for debugging
    logging.info("DataFrame data types:")
    for col in df.columns:
        logging.info(f"{col}: {df[col].dtype}")
    
    # Insert data
    success = insert_data(connection, df, columns, config['table'])
    
    # Close connection
    connection.close()
    logging.info("Database connection closed")
    
    if success:
        print("Data transfer completed successfully!")
        logging.info("Data transfer completed successfully")
    else:
        print("Data transfer failed. Check log file for details.")
        logging.error("Data transfer failed")

if __name__ == "__main__":
    main()