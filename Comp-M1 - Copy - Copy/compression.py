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

# Function to create tables for storing study and image metadata
def create_metadata_tables(connection):
    create_studies_table_query = '''
        CREATE TABLE IF NOT EXISTS studies (
            id SERIAL PRIMARY KEY,
            patient_id VARCHAR(10) NOT NULL,
            modality VARCHAR(50),
            folderpath VARCHAR(255) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            compressed BOOLEAN DEFAULT FALSE
        )
    '''
    create_images_table_query = '''
        CREATE TABLE IF NOT EXISTS images (
            id SERIAL PRIMARY KEY,
            study_id INTEGER REFERENCES studies(id),
            filename VARCHAR(255) NOT NULL,
            filepath VARCHAR(255) NOT NULL
        )
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(create_studies_table_query)
        cursor.execute(create_images_table_query)
        connection.commit()
        print("Metadata tables created successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error creating metadata tables: {error}")

# Function to generate a random patient ID
def generate_patient_id():
    # Generate a random 4-digit patient ID
    return str(random.randint(1000, 9999))

# Function to insert study metadata into PostgreSQL table
def insert_study_metadata(connection, patient_id, modality, folderpath):
    insert_study_query = '''
        INSERT INTO studies (patient_id, modality, folderpath)
        VALUES (%s, %s, %s)
        RETURNING id
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(insert_study_query, (patient_id, modality, folderpath))
        study_id = cursor.fetchone()[0]
        connection.commit()
        print(f"Metadata inserted for study with folderpath {folderpath}.")
        return study_id
    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting study metadata: {error}")
        return None

# Function to insert image metadata into PostgreSQL table
def insert_image_metadata(connection, study_id, filename, filepath):
    insert_image_query = '''
        INSERT INTO images (study_id, filename, filepath)
        VALUES (%s, %s, %s)
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(insert_image_query, (study_id, filename, filepath))
        connection.commit()
        print(f"Metadata inserted for image {filename}.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting image metadata: {error}")

# Function to compress a folder containing DICOM images using gzip
def compress_folder(folder_path, compressed_path):
    try:
        shutil.make_archive(compressed_path, 'gztar', folder_path)
        print(f"Folder {folder_path} compressed successfully.")
    except Exception as e:
        print(f"Error compressing folder {folder_path}: {e}")

# Function to update study metadata in the database
def update_study_metadata(connection, original_path, new_path):
    update_query = '''
        UPDATE studies
        SET folderpath = %s, compressed = True
        WHERE folderpath = %s
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(update_query, (new_path, original_path))
        connection.commit()
        print(f"Metadata updated for study with folderpath {original_path}.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error updating study metadata: {error}")

# Example usage
def main(input_directory, short_term_directory, long_term_directory, local_directory):
    os.makedirs(input_directory, exist_ok=True)
    os.makedirs(short_term_directory, exist_ok=True)
    os.makedirs(long_term_directory, exist_ok=True)
    os.makedirs(local_directory, exist_ok=True)

    # Connect to PostgreSQL database
    connection = connect_to_database()
    if connection:
        # Create metadata tables if not exists
        create_metadata_tables(connection)

        # Initialize folder timers for existing folders in short-term directory
        folder_timers = {}
        for study_folder in os.listdir(short_term_directory):
            folder_path = os.path.join(short_term_directory, study_folder)
            if os.path.isdir(folder_path):
                folder_timers[study_folder] = time.time()

        while True:
            # Move new study folders from input directory to short-term directory
            for study_folder in os.listdir(input_directory):
                study_path = os.path.join(input_directory, study_folder)
                if os.path.isdir(study_path):
                    print(f"Processing new study folder: {study_folder}")
                    # Move the folder to the short-term directory
                    shutil.move(study_path, os.path.join(short_term_directory, study_folder))
                    
                    # Start timer for the new folder
                    folder_timers[study_folder] = time.time()
                    
                    # Extract modality information from the first DICOM file in the folder
                    first_dicom_path = os.path.join(short_term_directory, study_folder, os.listdir(os.path.join(short_term_directory, study_folder))[0])
                    dataset = pydicom.dcmread(first_dicom_path)
                    modality = dataset.Modality
                    print(f"Modality extracted: {modality}")

                    # Generate a unique patient ID for each study
                    patient_id = generate_patient_id()
                    print(f"Generated patient ID: {patient_id}")

                    # Insert metadata for the moved folder with modality information
                    study_id = insert_study_metadata(connection, patient_id, modality, os.path.join(short_term_directory, study_folder))

                    # Insert metadata for each DICOM file in the study folder
                    for dicom_file in os.listdir(os.path.join(short_term_directory, study_folder)):
                        dicom_path = os.path.join(short_term_directory, study_folder, dicom_file)
                        insert_image_metadata(connection, study_id, dicom_file, dicom_path)

            # Check for folders in the short-term directory
            current_time = time.time()
            for study_folder in os.listdir(short_term_directory):
                folder_path = os.path.join(short_term_directory, study_folder)
                if os.path.isdir(folder_path):
                    if study_folder in folder_timers:
                        folder_age = current_time - folder_timers[study_folder]
                        if folder_age > 180:  # Delay of 3 minutes (180 seconds)
                            compressed_long_term_path = os.path.join(long_term_directory, study_folder)
                            compressed_local_path = os.path.join(local_directory, study_folder)
                            
                            # Compress the study folder and move to long-term storage
                            compress_folder(folder_path, compressed_long_term_path)
                            update_study_metadata(connection, folder_path, compressed_long_term_path + '.tar.gz')
                            print(f"Study {study_folder} compressed and moved to long-term storage.")
                            
                            # Compress the study folder and move to local storage
                            compress_folder(folder_path, compressed_local_path)
                            update_study_metadata(connection, folder_path, compressed_local_path + '.tar.gz')
                            print(f"Study {study_folder} compressed and moved to local storage.")

                            # Remove the original study folder
                            shutil.rmtree(folder_path)
                            print(f"Original study folder {folder_path} removed.")

                            del folder_timers[study_folder]

            # Sleep for a certain interval before checking again
            time.sleep(5)  # Check every 5 seconds (adjust as needed)

    # Close database connection
    if connection:
        connection.close()

if __name__ == "__main__":
    input_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\input'
    short_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\shortterm'
    long_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\longterm'
    local_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\local'

    main(input_directory, short_term_directory, long_term_directory, local_directory)
