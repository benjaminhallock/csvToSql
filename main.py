import os
import csv

def write_to_file(content, filename="output.sql"):
    with open(filename, 'a') as f:
        f.write(content + "\n")

def create_tables():
    create_event_table = """CREATE TABLE IF NOT EXISTS Event (
        eid INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(255),
        location VARCHAR(255),
        month VARCHAR(255)
    );"""

    create_participant_table = """CREATE TABLE IF NOT EXISTS Participant (
        pid INT PRIMARY KEY AUTO_INCREMENT,
        fname VARCHAR(255),
        lname VARCHAR(255)
    );"""

    create_model_table = """CREATE TABLE IF NOT EXISTS Model (
        mid INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(255)
    );"""

    create_medal_table = """CREATE TABLE IF NOT EXISTS Medal (
        Category VARCHAR(255),
        Year VARCHAR(255),
        Award VARCHAR(255),
        mid INT,
        pid INT,
        eid INT,
        FOREIGN KEY (eid) REFERENCES Event(eid),
        FOREIGN KEY (pid) REFERENCES Participant(pid),
        FOREIGN KEY (mid) REFERENCES Model(mid),
        PRIMARY KEY (mid, pid, eid)
    );"""

    # Write to file
    write_to_file(create_event_table)
    write_to_file(create_participant_table)
    write_to_file(create_model_table)
    write_to_file(create_medal_table)

def process_events_file(file, event_id_map):
    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Skip first line
        next(reader)

        for row in reader:
            if len(row) == 4:  # Expecting exactly 4 values for Events table
                #replace ' with '' to escape single quotes
                row = [x.replace("'", "''") for x in row]
                
                event_id, event_name, location, month = row
                event_id_map[event_name] = event_id
                event_insert = f'INSERT INTO Event (eid, name, location, month) VALUES ({event_id}, \'{event_name}\', \'{location}\', \'{month}\');'
                write_to_file(event_insert)
            else:
                print(f"Invalid row: {row}")

def process_participants_and_models(file, event_id_map):
    participant_id_map = {}
    model_id_map = {}

    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Skip first line
        next(reader)

        for row in reader:
            if len(row) == 6:  # Ensure exactly 6 values
                #replace ' with '' to escape single quotes
                row = [x.replace("'", "''") for x in row]
                year, location, medal, participant_fullname, miniature_name, category = row
                event_key = location

                participant_fname = participant_fullname.split()[0]
                participant_lname = " ".join(participant_fullname.split()[1:])
                
                # Use event name to look up event ID
                event_id = event_id_map.get(event_key)

                if event_id is None:
                    print(f"Event not found for key: {event_key}")
                    continue

                # Insert into Participant
                participant_name = f"{participant_fname} {participant_lname}"
                if participant_name not in participant_id_map:
                    participant_insert = f'INSERT INTO Participant (fname, lname) VALUES (\'{participant_fname}\', \'{participant_lname}\');'
                    write_to_file(participant_insert)
                    participant_id_map[participant_name] = len(participant_id_map) + 1
                pid = participant_id_map[participant_name]  # Retrieve participant ID

                # Insert into Model
                if miniature_name not in model_id_map:
                    model_insert = f'INSERT INTO Model (name) VALUES (\'{miniature_name}\');'
                    write_to_file(model_insert)
                    model_id_map[miniature_name] = len(model_id_map) + 1
               
                mid = model_id_map[miniature_name]  # Retrieve model ID
                # Check for existing medal entry to avoid duplicates
                medal_key = (mid, pid, event_id)
                if medal_key not in model_id_map:
                    # Insert into Medal
                    medal_insert = f'INSERT INTO Medal (Category, Year, Award, mid, pid, eid) VALUES (\'{category}\', \'{year}\', \'{medal}\', {mid}, {pid}, {event_id});'
                    write_to_file(medal_insert)
                    # model_id_map[medal_key] = True  # Mark this medal key as used
                else:
                    print(f"Duplicate medal entry for {miniature_name}, {participant_name}, {event_id}. Skipping insertion.")
            else:
                print(f"Invalid row: {row}")

def process_files(file_list):
    event_id_map = {}

    # Process the first file for Events
    process_events_file(file_list[0], event_id_map)

    # Now process the remaining files for Participants, Models, and Medals
    for file in file_list[1:]:
        process_participants_and_models(file, event_id_map)

if __name__ == "__main__":
    files = ['1.txt', '2.txt', '3.txt']  # Adjust the file names as needed

    # Clear or create output file
    if os.path.exists("output.sql"):
        os.remove("output.sql")

    # Create SQL tables
    create_tables()

    # Process files and write SQL inserts
    process_files(files)