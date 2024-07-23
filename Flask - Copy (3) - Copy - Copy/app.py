from flask import Flask, render_template, send_file, request, redirect, url_for, session
from flask_socketio import SocketIO
from flask_bcrypt import Bcrypt
import psycopg2
import os
import gzip
from psycopg2 import IntegrityError
import re
import shutil

app = Flask(__name__)
app.config['SECRET_KEY'] = 'qwerty12345{}'
socketio = SocketIO(app)
bcrypt = Bcrypt(app)

# Database connection settings
DB_NAME = "image"
DB_USER = "postgres"
DB_PASSWORD = "SQLfatima@31"
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

# Function to validate email format
def validate_email(email):
    # Simple pattern to validate Gmail addresses
    pattern = r'^[a-zA-Z0-9._%+-]+@gmail\.com$'
    return re.match(pattern, email)

# Function to validate password format
def validate_password(password):
    # Password must be at least 8 characters long and contain at least one digit and one special character
    return len(password) >= 8 and any(char.isdigit() for char in password) and any(char in '!@#$%^&*()-_=+[]{};:,.<>?/|' for char in password)

# Route for user registration
@app.route('/register', methods=['GET', 'POST'])
def register():
    error_msg = None
    success_msg = None

    if request.method == 'POST':
        # Get form data
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form['confirm_password']

        # Validate email format
        if not validate_email(email):
            error_msg = 'Invalid email format! Enter a valid Gmail address.'
        # Check if the email is already registered
        elif is_email_registered(email):
            error_msg = 'This email address is already registered. Please use a different email address.'
        # Validate password difficulty
        elif not validate_password(password):
            error_msg = 'Password must be at least 8 characters long and contain at least one digit and one special character.'
        # Check if passwords match
        elif password != confirm_password:
            error_msg = 'Passwords do not match.'
        else:
            # Hash the password
            hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')

            # Insert user data into the database
            cursor.execute("INSERT INTO users (username, email, password, confirmed_password) VALUES (%s, %s, %s, %s)", (username, email, hashed_password, hashed_password))
            conn.commit()

            # Set success message
            success_msg = 'Successfully registered!'

            # Redirect to login page after successful registration
            return redirect(url_for('login'))

    # Render the registration form with messages
    return render_template('register.html', error_msg=error_msg, success_msg=success_msg)

def is_email_registered(email):
    # Query the database to check if the email is already registered
    cursor.execute("SELECT COUNT(*) FROM users WHERE email = %s", (email,))
    return cursor.fetchone()[0] > 0

# Route for user login
@app.route('/login', methods=['GET', 'POST'])
def login(): 
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        # Fetch user data from the database
        cursor.execute("SELECT id, username, email, password FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()

        if user:
            # Check if passwords match
            if bcrypt.check_password_hash(user[3], password):
                # Store user session data
                session['user_id'] = user[0]
                session['username'] = user[1]
                return redirect(url_for('display_metadata'))
            else:
                # Invalid password
                return 'Invalid email or password'
        else:
            # User with entered email does not exist
            return 'User does not exist'

    return render_template('login.html')


@app.route('/logout', methods=['POST'])
def logout():
    # Clear user session data
    session.clear()
    return redirect(url_for('login'))


# Route for displaying image metadata
# Route for displaying image metadata
@app.route('/')
def display_metadata():
    if 'user_id' in session:
        # Fetch data only from the studies table
        cursor.execute("SELECT id, patient_id, modality, folderpath, timestamp, compressed FROM studies")
        data = cursor.fetchall()
        username = session['username']
        
        # Pre-process data to extract folder names
        data_with_folder_info = []
        for row in data:
            study_id, patient_id, modality, folderpath, timestamp, compressed = row
            folder_name = os.path.basename(folderpath)
            data_with_folder_info.append((study_id, patient_id, modality, folder_name, folderpath, timestamp, compressed))
        
        # Pass pre-processed data to the template for rendering
        return render_template('index.html', studies=data_with_folder_info, username=username)
    else:
        return redirect(url_for('login'))

@app.route('/download/<int:study_id>')
def download_study(study_id):
    # Authentication check
    if 'user_id' not in session:
        return redirect(url_for('login'))

    # Fetch the study folderpath and compression status from the studies table
    cursor.execute("SELECT folderpath, compressed FROM studies WHERE id = %s", (study_id,))
    study_data = cursor.fetchone()

    if study_data:
        folderpath = study_data[0]
        compressed = study_data[1]

        # Determine the full path based on folderpath
        if 'longterm' in folderpath:
            folder_path = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\longterm'
        elif 'shortterm' in folderpath:
            folder_path = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\shortterm'
        else:
            return "Invalid folderpath"

        full_path = os.path.join(folder_path, folderpath)

        # Check if the study is compressed (.gz)
        if compressed:
            # Decompress the study
            uncompressed_path = full_path[:-3]  # Remove the .gz extension
            with gzip.open(full_path, 'rb') as f_in:
                with open(uncompressed_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            full_path = uncompressed_path  # Update full path to uncompressed folder

        # Create the "downloaded" folder if it doesn't exist
        downloaded_folder = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\downloaded'
        if not os.path.exists(downloaded_folder):
            os.makedirs(downloaded_folder)

        # Move the folder to the "downloaded" folder
        downloaded_path = os.path.join(downloaded_folder, os.path.basename(full_path))

        # If a file with the same name already exists, rename the new file
        if os.path.exists(downloaded_path):
            base_name, extension = os.path.splitext(os.path.basename(full_path))
            downloaded_path = os.path.join(downloaded_folder, base_name + "_new" + extension)

        # Move the folder to the "downloaded" folder
        shutil.move(full_path, downloaded_path)

        # Download the folder from the "downloaded" folder
        return send_file(downloaded_path, as_attachment=True)
    else:
        return "Study not found"
    
# Route for handling refresh button click event
@socketio.on('refresh')
def refresh_data():
    # Fetch the latest data from the database
    cursor.execute("SELECT id, modality, filename, filepath, timestamp, compressed, patient_id FROM image_metadata")
    data = cursor.fetchall()
    # Emit the updated data to the client
    socketio.emit('update_data', data)

if __name__ == '__main__':
    socketio.run(app, debug=True)
