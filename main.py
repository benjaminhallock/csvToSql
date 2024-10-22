import os
import csv


def write_to_file(content, filename="output.sql"):
    with open(filename, 'a') as f:
        f.write(content + "\n")


def create_tables():
    create_event_table = """CREATE TABLE IF NOT EXISTS Event (
        eid INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        location VARCHAR(255),
        month VARCHAR(50)
    );"""

    create_participant_table = """CREATE TABLE IF NOT EXISTS Participant (
        pid INT AUTO_INCREMENT PRIMARY KEY,
        fname VARCHAR(255),
        lname VARCHAR(255)
    );"""

    create_model_table = """CREATE TABLE IF NOT EXISTS Model (
        mid INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255)
    );"""

    create_medal_table = """CREATE TABLE IF NOT EXISTS Medal (
        Category VARCHAR(255),
        Year YEAR,
        Award VARCHAR(50),
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
                event_id, event_name, location, month = row
                event_id_map[(event_name)] = event_id
                event_insert = f'INSERT INTO Event (eid, name, location, month) VALUES ({event_id}, "{event_name}", "{location}", "{month}");'
                write_to_file(event_insert)
            else:
                print(f"Invalid row: {row}")


def process_participants_and_models(file, event_id_map):
    participant_id_map = {}

    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)

        # Skip first line
        next(reader)

        for row in reader:
            if len(row) == 6:  # Ensure exactly 6 values
                year, location, medal, participant_fullname, miniature_name, category = row
                event_key = location

                participant_fname = participant_fullname.split()[0]
                participant_lname = " ".join(participant_fullname.split()[1:])
                
                if event_key not in event_id_map:
                    print(f"Event not found for key: {event_key}")
                    continue

                # Insert into Participant
                participant_name = f"{participant_fname} {participant_lname}"
                if participant_name not in participant_id_map:
                    participant_insert = f'INSERT INTO Participant (fname, lname) VALUES ("{participant_fname}", "{participant_lname}");'
                    write_to_file(participant_insert)
                    participant_id_map[participant_name] = len(participant_id_map) + 1

                # Insert into Model
                event_id_fk = event_id_map[event_key]
                participant_id_fk = participant_id_map[participant_name]

                model_insert = f'INSERT INTO Model (name) VALUES ("{miniature_name}");'
                write_to_file(model_insert)
                model_id_fk = participant_id_fk  # Assuming mid is auto-incremented, and you need the same participant

                # Insert into Medal
                medal_insert = f'INSERT INTO Medal (Category, Year, Award, mid, pid, eid) VALUES ("{category}", "{year}", "{medal}", {model_id_fk}, {participant_id_fk}, {event_id_fk});'
                write_to_file(medal_insert)
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