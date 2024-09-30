import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_sqlite_to_mariadb(sqlite_db_path, output_sql_file):
    logging.debug(f'Starting conversion from SQLite database: {sqlite_db_path}')

    try:
        sqlite_conn = sqlite3.connect(sqlite_db_path)
        cursor = sqlite_conn.cursor()
        logging.debug('Successfully connected to SQLite database.')
    except Exception as e:
        logging.error(f'Error connecting to SQLite database: {e}')
        return

    try:
        with open(output_sql_file, 'w') as f:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            logging.debug(f'Tables found: {tables}')

            for table_name in tables:
                table_name = table_name[0]
                logging.debug(f'Processing table: {table_name}')
                f.write(f'-- Table structure for {table_name}\n')

                cursor.execute(f'SELECT sql FROM sqlite_master WHERE name="{table_name}";')
                create_statement = cursor.fetchone()[0]
                create_statement = create_statement.replace("AUTOINCREMENT", "")
                create_statement = create_statement.replace("INTEGER PRIMARY KEY", "INT PRIMARY KEY AUTO_INCREMENT")
                f.write(f'{create_statement};\n\n')

                cursor.execute(f'SELECT * FROM {table_name};')

                while True:
                    rows = cursor.fetchmany(5000)
                    if not rows:
                        break

                    for row in rows:
                        values = ', '.join([f'"{str(value)}"' if isinstance(value, str) else str(value) for value in row])
                        f.write(f'INSERT INTO {table_name} VALUES ({values});\n')

                f.write('\n')

        logging.debug(f'Conversion complete! SQL file saved as {output_sql_file}')

    except Exception as e:
        logging.error(f'Error during conversion: {e}')
    finally:
        sqlite_conn.close()
        logging.debug('SQLite connection closed.')

if __name__ == '__main__':
    sqlite_db_path = 'database.db'
    output_sql_file = 'converted.sql'
    convert_sqlite_to_mariadb(sqlite_db_path, output_sql_file)
