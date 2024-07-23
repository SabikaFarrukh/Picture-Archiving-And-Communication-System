import os
import shutil
import time
import gzip
import pydicom
import psycopg2
from datetime import datetime
import random

# Function to connect to PostgreSQL database
def connect_to_database():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="SQLfatima@31",
            host="localhost",
            port="5432",
            database="image"
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None

# Function to create table for storing image metadata
def create_metadata_table(connection):
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS image_metadata (
            id SERIAL PRIMARY KEY,
            modality VARCHAR(50),
            filename VARCHAR(255) NOT NULL,
            filepath VARCHAR(255) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            compressed BOOLEAN DEFAULT FALSE,
            patient_id VARCHAR(10)  -- New column for patient ID
        )
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("Metadata table created successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error creating metadata table: {error}")

# Function to generate a random patient ID
def generate_patient_id():
    # Generate a random 4-digit patient ID
    return str(random.randint(1000, 9999))

# Function to insert metadata into PostgreSQL table
def insert_metadata(connection, filename, filepath, modality, patient_id):
    # Adjust the filepath to be relative to the short-term directory
    short_term_filepath = os.path.join(short_term_directory, filename)
    
    insert_query = '''
        INSERT INTO image_metadata (filename, filepath, modality, patient_id)
        VALUES (%s, %s, %s, %s)
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(insert_query, (filename, short_term_filepath, modality, patient_id))
        connection.commit()
        print(f"Metadata inserted for {filename}.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting metadata: {error}")

# def get_storage_usage(directory):
#     usage = shutil.disk_usage(directory)
#     total_space = usage.total
#     used_space = usage.used
#     return (total_space, used_space)

# Function to compress DICOM image using gzip
def compress_dicom(dicom_path, compressed_path):
    try:
        with open(dicom_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        print(f"Error compressing DICOM image: {e}")

# Function to update image metadata in the database
def update_metadata(connection, original_path, new_path):
    update_query = '''
        UPDATE image_metadata
        SET filepath = %s, compressed = True
        WHERE filepath = %s
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(update_query, (new_path, original_path))
        connection.commit()
        print(f"Metadata updated for {original_path}.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error updating metadata: {error}")

# Rest of your code remains unchanged...

# Example usage
def main(input_directory, short_term_directory, long_term_directory, local_directory):
    os.makedirs(input_directory, exist_ok=True)
    os.makedirs(short_term_directory, exist_ok=True)
    os.makedirs(long_term_directory, exist_ok=True)
    os.makedirs(local_directory, exist_ok=True)

    # Connect to PostgreSQL database
    connection = connect_to_database()
    if connection:
        # Create metadata table if not exists
        create_metadata_table(connection)

        # Initialize file timers for existing files in short-term directory
        file_timers = {}
        for dicom_file in os.listdir(short_term_directory):
            file_timers[dicom_file] = time.time()

        while True:
            # Move new DICOM images from input directory to short-term directory
            for dicom_file in os.listdir(input_directory):
                dicom_path = os.path.join(input_directory, dicom_file)
                if os.path.isfile(dicom_path) and dicom_file.endswith('.dcm'):
                    # Move the file to the short-term directory
                    shutil.move(dicom_path, os.path.join(short_term_directory, dicom_file))
                    
                    # Start timer for the new file
                    file_timers[dicom_file] = time.time()
                    
                    # Extract modality information from DICOM file
                    dataset = pydicom.dcmread(os.path.join(short_term_directory, dicom_file))
                    modality = dataset.Modality

                    # Generate a unique patient ID for each image
                    patient_id = generate_patient_id()

                    # Insert metadata for the moved file with modality information
                    insert_metadata(connection, dicom_file, dicom_path, modality, patient_id)

            # Check for files in the short-term directory
            current_time = time.time()
            for dicom_file in os.listdir(short_term_directory):
                dicom_path = os.path.join(short_term_directory, dicom_file)
                if os.path.isfile(dicom_path):
                    if dicom_file in file_timers:
                        file_age = current_time - file_timers[dicom_file]
                        if file_age > 180:  # Delay of 3 minutes (180 seconds)
                            filename = os.path.splitext(os.path.basename(dicom_path))[0]
                            compressed_long_term_path = os.path.join(long_term_directory, f'{filename}.gz')
                            compressed_local_path = os.path.join(local_directory, f'{filename}.gz')
                            
                            # Compress the DICOM file and move to long-term storage
                            compress_dicom(dicom_path, compressed_long_term_path)
                            update_metadata(connection, dicom_path, compressed_long_term_path)
                            print(f"Image {filename} compressed and moved to long-term storage.")
                            
                            # Compress the DICOM file and move to local storage
                            compress_dicom(dicom_path, compressed_local_path)
                            update_metadata(connection, dicom_path, compressed_local_path)
                            print(f"Image {filename} compressed and moved to local storage.")

                            # Remove the original DICOM file
                            os.remove(dicom_path)
                            print(f"Original DICOM file {dicom_path} removed.")

                            del file_timers[dicom_file]

            # Sleep for a certain interval before checking again
            time.sleep(5)  # Check every 5 seconds (adjust as needed)


# def main(input_directory, short_term_directory, long_term_directory, local_directory):
#     os.makedirs(input_directory, exist_ok=True)
#     os.makedirs(long_term_directory, exist_ok=True)
#     os.makedirs(local_directory, exist_ok=True)

#     # Connect to PostgreSQL database
#     connection = connect_to_database()
#     if connection:
#         # Create metadata table if not exists
#         create_metadata_table(connection)

#         while True:
#             # Check storage usage of the short-term directory
#             total_space, used_space = get_storage_usage(short_term_directory)
#             usage_percentage = (used_space / total_space) * 100

#             # If usage exceeds threshold, move files to long-term and local directories
#             if usage_percentage >= 70:
#                 for dicom_file in os.listdir(short_term_directory):
#                     dicom_path = os.path.join(short_term_directory, dicom_file)
#                     if os.path.isfile(dicom_path) and dicom_file.endswith('.dcm'):
#                         filename = os.path.splitext(os.path.basename(dicom_path))[0]
#                         compressed_long_term_path = os.path.join(long_term_directory, f'{filename}.gz')
#                         compressed_local_path = os.path.join(local_directory, f'{filename}.gz')

#                         # Compress the DICOM file and move to long-term storage
#                         compress_dicom(dicom_path, compressed_long_term_path)
#                         update_metadata(connection, dicom_path, compressed_long_term_path)
#                         print(f"Image {filename} compressed and moved to long-term storage.")

#                         # Compress the DICOM file and move to local storage
#                         compress_dicom(dicom_path, compressed_local_path)
#                         update_metadata(connection, dicom_path, compressed_local_path)
#                         print(f"Image {filename} compressed and moved to local storage.")

#                         # Remove the original DICOM file
#                         os.remove(dicom_path)
#                         print(f"Original DICOM file {dicom_path} removed.")

#             # Sleep for a certain interval before checking again
#             time.sleep(60)  # Check every minute (adjust as needed)

#     # Close database connection
#     if connection:
#         connection.close()

# if __name__ == "__main__":
#     input_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\input'
#     short_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\shortterm'
#     long_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\longterm'
#     local_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\local'

#     main(input_directory, short_term_directory, long_term_directory, local_directory)

    # Close database connection
    if connection:
        connection.close()


if __name__ == "__main__":
    input_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\input'
    short_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\shortterm'
    long_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\longterm'
    local_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1\local'

    main(input_directory, short_term_directory, long_term_directory, local_directory)
