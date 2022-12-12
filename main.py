import sqlite3
import traceback
import re
from termcolor import colored, cprint
import pandas as pd
from tabulate import tabulate


def print_status(str):  # helper function for printing status
    cprint(str, "cyan")


def print_error(str):  # helper function for printing error
    cprint(str, "red")


def print_table(str):  # helper function for printing table
    cprint(str, "green")


def format_prompt(str):  # helper function for formatting prompts
    return colored(str, "yellow")


class Main:
    exit_cmd = ".exit"
    db_name = "airline.db"
    table_names = ["flight", "aircraft", "pilot"]

    def __init__(self):
        self.cmd_types = {
            "insert": self.insert,
            "update": self.update,
            "delete": self.delete,
            "search": self.search,
            "view": self.view,
            "stat": self.stat,
            ".man": self.showManual,
        }
        print_status("Starting airline database control CLI application...")
        # initialize databases
        self.initDB()

    # initialise the DB and some of the contents if they do not exist
    def initDB(self):
        try:
            # initialise connection with sqlite
            self.conn = sqlite3.connect(self.db_name)
            print_status(f"Successfully connected with DB: {self.db_name}")
            self.db_cursor = self.conn.cursor()
            self.db_cursor.execute("PRAGMA foreign_keys = ON")

            # initialise tables
            self.db_cursor.execute("\
                CREATE TABLE IF NOT EXISTS aircraft (\
                    aircraft_id INTEGER PRIMARY KEY,\
                    age INTEGER NOT NULL,\
                    capacity INTEGER NOT NULL\
                );")
            print_status("Successfully connected with table: aircraft")

            self.db_cursor.execute("\
                CREATE TABLE IF NOT EXISTS pilot (\
                    staff_id INTEGER PRIMARY KEY,\
                    last_name TEXT NOT NULL,\
                    first_name TEXT NOT NULL\
                );")
            print_status("Successfully connected with table: pilot")

            self.db_cursor.execute("\
                CREATE TABLE IF NOT EXISTS flight (\
                    flight_id TEXT PRIMARY KEY,\
                    start_airport TEXT NOT NULL,\
                    destination_airport TEXT NOT NULL,\
                    departure_date TEXT DEFAULT '2022-01-01' NOT NULL,\
                    aircraft_id INTEGER NOT NULL,\
                    pilot_1 INTEGER NOT NULL,\
                    pilot_2 INTEGER,\
                    FOREIGN KEY (aircraft_id)\
                        REFERENCES aircraft (aircraft_id)\
                    FOREIGN KEY (pilot_1)\
                        REFERENCES pilot (staff_id)\
                    FOREIGN KEY (pilot_2)\
                        REFERENCES pilot (staff_id)\
                );")
            print_status("Successfully connected with table: flight")

            # initialise some data in pilot table
            if not self.db_cursor.execute("SELECT COUNT(1) FROM pilot;").fetchone()[0]:
                init_pilot_query = "INSERT INTO pilot (staff_id, last_name, first_name) VALUES (:staff_id, :last_name, :first_name);"
                pilot_data = [
                    {"staff_id": "1", "last_name": "beckham", "first_name": "david"},
                    {"staff_id": "2", "last_name": "kane", "first_name": "harry"}
                ]
                self.db_cursor.executemany(init_pilot_query, pilot_data)

            # initialise some data in aircraft table
            if not self.db_cursor.execute("SELECT COUNT(1) FROM aircraft;").fetchone()[0]:
                init_aircraft_query = "INSERT INTO aircraft (aircraft_id, age, capacity) VALUES (:aircraft_id, :age, :capacity);"
                aircraft_data = [
                    {"aircraft_id": "1", "age": 5, "capacity": 420},
                    {"aircraft_id": "2", "age": 9, "capacity": 110}
                ]
                self.db_cursor.executemany(init_aircraft_query, aircraft_data)

            self.conn.commit()
            print_status("Successfully initialize tables")

        except Exception as e:
            print_error(e)
            print(traceback.format_exc())

    # entry point of the program
    def execute(self):
        while cmd := input(format_prompt(f"Enter a command (type .man to open manual): ")).strip():
            if cmd == self.exit_cmd:
                self.db_cursor.close()
                self.conn.close()
                break
            if op := self.cmd_types.get(cmd):
                op()
            else:
                print_error("Invalid command: type .man to open user manual.")

    def insert(self):
        try:
            insert_dict = {
                "flight": {
                    "prompt": "Input data in the following format: flight_id, start_airport, destination_airport, departure_date (YYYY-MM-DD), aircraft_id, pilot_1, pilot_2\nNote:\n\t- flight_id must be unique\n\t- Only pilot_2 is optional. If you skip this field, still input a comma to separate the field with other fields\n\t- Use ; to separate rows when inputting more than 1 row.\n",
                    "query": "INSERT INTO flight (flight_id, start_airport, destination_airport, departure_date, aircraft_id, pilot_1, pilot_2) VALUES (?, ?, ?, ?, ?, ?, ?)"
                },
                "pilot": {
                    "prompt": "Input data in the following format: staff_id, last_name, first_name\nNote:\n\t- staff_id must be unique\n\t- all fields are compulsory\n\t- Use ; to separate rows when inputting more than 1 row.\n",
                    "query": "INSERT INTO pilot (staff_id, last_name, first_name) VALUES (?, ?, ?)"
                },
                "aircraft": {
                    "prompt": "Input data in the following format: aircraft_id, age, capacity\nNote:\n\t- aircraft_id must be unique\n\t- all fields are compulsory\n\t- Use ; to separate rows when inputting more than 1 row.\n",
                    "query": "INSERT INTO aircraft (aircraft_id, age, capacity) VALUES (?, ?, ?)"
                }
            }

            # first prompt to confirm which table to insert to
            prompt = format_prompt(
                f"Which table to insert to? ({'/ '.join(list(insert_dict.keys()))}): ")
            option = input(prompt).strip()
            if not option or not (option in list(insert_dict.keys())):
                print_error(
                    f"Invalid input; Only accept: {'/ '.join(list(insert_dict.keys()))}")
                self.insert()
                return

            # second prompt to ask for data input
            data = []
            userInput = input(format_prompt(insert_dict[option]["prompt"]))
            for line in userInput.split(";"):
                row = []
                for field in line.split(","):
                    field = field.strip()
                    if field == "":
                        row.append(None)
                    else:
                        row.append(field)
                # validate the row
                if error := self.validateData(row, option):
                    print_error(error)
                    self.insert()
                    return
                data.append(row)

            self.db_cursor.executemany(insert_dict[option]["query"], data)
            self.conn.commit()
            print_status('Data successfully inserted.')
        except Exception as e:
            print_error(e)
            print(traceback.format_exc())
            self.insert()

    def update(self):
        try:
            update_dict = {
                "flight": {
                    "prompt": "Update flight data in the following format: flight_id, attribute_1=attribute_1_new, attribute_2=attribute_2_new...\nNotes:\n\t- e.g. BA123, start_airport=BLN\n\t- flight_id is not editable\n\t- to remove pilot_2 value of a row, pass in 'pilot_2='\n",
                    "query": "UPDATE flight SET conds_placeholder WHERE flight_id=?",
                    "pk": "flight_id"
                },
                "pilot": {
                    "prompt": "Update pilot data in the following format: staff_id, attribute_1=attribute_1_new, attribute_2=attribute_2_new...\nNotes:\n\t- e.g. 1, last_name=smith\n\t- staff-id is not editable\n",
                    "query": "UPDATE pilot SET conds_placeholder WHERE staff_id=?",
                    "pk": "staff_id"
                },
                "aircraft": {
                    "prompt": "Update aircraft data in the following format: aircraft_id, attribute_1=attribute_1_new, attribute_2=attribute_2_new...\nNotes:\n\t- e.g. 1, age=1\n\t- aircraft_id is not editable\n",
                    "query": "UPDATE aircraft SET conds_placeholder WHERE aircraft_id=?",
                    "pk": "aircraft_id"
                }
            }

            # first prompt to confirm which table to update
            prompt = format_prompt(
                f"Which table to update? ({'/ '.join(list(update_dict.keys()))}): ")
            option = input(prompt).strip()
            if not option or not (option in list(update_dict.keys())):
                print_error(
                    f"Invalid input; Only accept: {'/ '.join(list(update_dict.keys()))}")
                self.update()
                return

            # second prompt to get data to update
            data = [field.strip() for field in input(
                format_prompt(update_dict[option]["prompt"])).split(",")]

            # data validation
            if error := self.validateUpdateData(data, update_dict, option):
                print_error(error)
                self.update()
                return

            # string manipulation to create the sql query string
            conds = []
            params = []
            for arg in data[1:]:
                _split = arg.split("=")
                conds.append(_split[0].strip() + "=?")
                if _split[1].strip():
                    params.append(_split[1].strip())
                else:
                    params.append(None)
            _query = update_dict[option]["query"].replace(
                "conds_placeholder", ",".join(conds))
            params.append(data[0])

            self.db_cursor.execute(_query, params)
            self.conn.commit()
            print_status(f'Successfully updated {option} {data[0]}.')
        except Exception as e:
            print_error(e)
            print(traceback.format_exc())
            self.update()

    def delete(self):
        try:
            # first prompt to confirm which table to delete
            prompt = format_prompt(
                f"Which table to delete? ({'/ '.join(self.table_names)}): ")
            option = input(prompt).strip()
            if not option or not (option in self.table_names):
                print_error(
                    f"Invalid input; Only accept: {'/ '.join(self.table_names)}")
                self.delete()
                return

            # second prompt to get the unique id that corresponds to the item to delete
            second_prompt = format_prompt(
                "Input the unique identifier of the item to delete: ")

            # data validation
            input_id = input(second_prompt).strip()

            pk_name = self.getPKName(option)
            if err := self.validateValueInPK(input_id, option, pk_name):
                print_error(err)
                self.delete()
                return

            _query = f"DELETE FROM {option} WHERE {pk_name} = ?"
            self.db_cursor.execute(_query, [input_id])
            self.conn.commit()
            print_status(
                f'Successfully delete {input_id} from {option}. Here is the latest view of {option}:')
            self.view(option)
        except Exception as e:
            if e.args == ("FOREIGN KEY constraint failed", ):
                print_error(
                    f"This item is currently used in the flight table.\nUpdate the flight table first.")
            else:
                print_error(e)
                print(traceback.format_exc())
                self.delete()

    # implement filter
    def search(self):
        try:
            search_dict = {
                "1": {
                    "description": "filter flight by flight_id",
                    "args": ["flight_id"],
                    "table": "flight"
                },
                "2": {
                    "description": "filter flight by destination_aiport",
                    "args": ["destination_airport"],
                    "table": "flight"
                },
                "3": {
                    "description": "filter flight by departure_date",
                    "args": ["departure_date"],
                    "table": "flight"
                },
                "4": {
                    "description": "filter flight by departure_date and destination_airport",
                    "args": ["departure_date", "destination_airport"],
                    "table": "flight"
                },
                "5": {
                    "description": "filter aircraft by aircraft_id",
                    "args": ["aircraft_id"],
                    "table": "aircraft"
                },
                "6": {
                    "description": "filter aircraft by age",
                    "args": ["age"],
                    "table": "aircraft"
                },
                "7": {
                    "description": "filter pilot by staff_id",
                    "args": ["staff_id"],
                    "table": "pilot"
                },
                "8": {
                    "description": "filter pilot by last_name",
                    "args": ["last_name"],
                    "table": "pilot"
                },
                "9": {
                    "description": "filter pilot by first_name",
                    "args": ["first_name"],
                    "table": "pilot"
                },
                "10": {
                    "description": "filter pilot by last name and first_name",
                    "args": ["last_name", "first_name"],
                    "table": "pilot"
                }
            }

            # first prompt to ask user the type of search they want to conduct
            prompt = format_prompt(f"""\
                        \nSelect the filter you wish to apply (input 1-10):\
                        \n\t1. {search_dict["1"]["description"]}\
                        \n\t2. {search_dict["2"]["description"]}\
                        \n\t3. {search_dict["3"]["description"]}\
                        \n\t4. {search_dict["4"]["description"]}\
                        \n\t5. {search_dict["5"]["description"]}\
                        \n\t6. {search_dict["6"]["description"]}\
                        \n\t7. {search_dict["7"]["description"]}\
                        \n\t8. {search_dict["8"]["description"]}\
                        \n\t9. {search_dict["9"]["description"]}\
                        \n\t10. {search_dict["10"]["description"]}\
                    """)
            print(prompt)
            option = input().strip()

            if not option or not (option in list(search_dict.keys())):
                print_error("Invalid input")
                self.search()
                return

            input_data = {}
            # second prompt to ask user for the value to filter by
            for arg in search_dict[option]["args"]:
                input_data[arg] = input(
                    format_prompt(f"Enter {arg}: ")).strip()

            # manipulate strings to create query
            conds = " AND ".join(
                [f"{arg} = :{arg}" for arg in search_dict[option]["args"]])
            _query = f"SELECT * FROM {search_dict[option]['table']} WHERE {conds}"
            print_table(
                f"Table Description: {search_dict[option]['description']}")
            self.display_table(_query, input_data)

        except Exception as e:
            print_error(e)
            print(traceback.format_exc())
            self.search()

    # def alter(self):
    #     raise NotImplementedError()

    def view(self, option=False):
        try:
            # first prompt to user
            prompt = format_prompt(
                "Input (flight / pilot / aircraft / all):\n")
            query_dict = {
                "flight": "SELECT * FROM flight",
                "pilot": "SELECT * FROM pilot",
                "aircraft": "SELECT * FROM aircraft",
                "all": "SELECT flight_id, start_airport, destination_airport, departure_date, a.aircraft_id, a.age AS aircraft_age, a.capacity AS aircraft_capacity, pilot_1, p1.first_name || ' ' || p1.last_name AS pilot1_name, pilot_2, p2.first_name || ' ' || p2.last_name AS pilot2_name\
                        FROM flight \
                        INNER JOIN aircraft AS a ON flight.aircraft_id = a.aircraft_id \
                        INNER JOIN pilot AS p1 ON flight.pilot_1 = p1.staff_id \
                        LEFT JOIN pilot AS p2 ON flight.pilot_2 = p2.staff_id"
            }

            # if the option agrument is not provided, prompt user to select the table to view
            if not option:
                option = input(prompt).strip()

            if _query := query_dict.get(option, False):
                self.display_table(_query)
            else:
                print_error(
                    "Invalid input; Only accept: flight/ pilot/ aircraft/ all")
                self.view()

        except Exception as e:
            print_error(e)
            print(traceback.format_exc())
            self.view()

    def stat(self, option=False):
        try:
            stat_dict = {
                "1": {
                    "description": "total number of flight (group by destination)",
                    "query": "SELECT destination_airport, COUNT(*) AS total_flight FROM flight GROUP BY destination_airport",
                },
                "2": {
                    "description": "total number of flight (group by month)",
                    "query": "SELECT month, COUNT(*) AS total_flight FROM\
                        (SELECT substr(departure_date, 6, 2) AS month FROM flight) GROUP BY month ORDER BY month",
                },
                "3": {
                    "description": "totaL number of flight (group by month and destination)",
                    "query": "SELECT month, destination_airport, COUNT(*) AS total_flight FROM\
                        (SELECT substr(departure_date, 6, 2) AS month, destination_airport FROM flight)\
                        GROUP BY month, destination_airport\
                        ORDER BY month",
                },
                "4": {
                    "description": "total pilot duty (group by pilot)",
                    "query": "SELECT staff_id, first_name || ' ' || last_name AS name, pilot_duty_count FROM pilot INNER JOIN \
                        (\
                            SELECT pilot_id, SUM(pilot_duty_count) AS pilot_duty_count FROM\
                                (\
                                    SELECT pilot_1 AS pilot_id, COUNT(*) AS pilot_duty_count FROM flight GROUP BY pilot_1\
                                    UNION ALL\
                                    SELECT pilot_2 AS pilot_id, COUNT(*) AS pilot_duty_count FROM flight GROUP BY pilot_2\
                                )\
                            GROUP BY pilot_id ORDER BY pilot_id\
                        )\
                        ON pilot_id = staff_id\
                        ORDER BY pilot_duty_count DESC"
                },
                "5": {
                    "description": "average aircraft age",
                    "query": "SELECT AVG(age) AS average_age FROM aircraft"
                },
            }

            # if the option agrument is not provided, prompt user to select which statistics to show
            if not option:
                prompt = format_prompt(f"""\
                    \nSelect the statistics you wish to view (input 1-5):\
                    \n\t1. {stat_dict["1"]["description"]}\
                    \n\t2. {stat_dict["2"]["description"]}\
                    \n\t3. {stat_dict["3"]["description"]}\
                    \n\t4. {stat_dict["4"]["description"]}\
                    \n\t5. {stat_dict["5"]["description"]}\
                """)
                print(prompt)
                option = input().strip()

            # check if the input is valid
            if not option or not (option in list(stat_dict.keys())):
                print_error("Invalid input")
                self.stat()
                return

            # separate handling for option 5 - which display a single value instead of a table
            if option == "5":
                avg_aircraft_age = self.db_cursor.execute(
                    stat_dict[option]["query"]).fetchone()[0]
                print_table(f"Average aircraft age is {avg_aircraft_age:.2f}")
            else:
                print_table(
                    f"Table Description: {stat_dict[option]['description']}")
                self.display_table(stat_dict[option]["query"])

        except Exception as e:
            print_error(e)
            print(traceback.format_exc())
            self.stat()

    # display the user manual
    def showManual(self):
        print("""
        insert: insert one or more row(s) to any of the tables
        update: update an item at any of the tables
        delete: delete an item by its unique identifier at any of the tables; show remaining data
        view: view any of the table or a joined overall table
        stat: view a range of statistics
        search: search for the an item using a range of attributes
        .man: open manual page
        .exit: exit the program
        """)

    def validateData(self, data, table_name):
        attributes = self.db_cursor.execute(
            f"PRAGMA table_info({table_name})").fetchall()
        # check arg length (should match the number of columns)
        if len(data) != len(attributes):
            return f"Invalid data input: incorrect number of arguments"
        for idx, attr in enumerate(attributes):
            if not self.validateField(data[idx], attr, table_name):
                return f"Invalid data input for attribute: {attr[1]}"

    def validateUpdateData(self, data, dict, table_name):
        # check arg length (>= 2)
        if len(data) < 2:
            return "Invalid input: supply at least 2 agruments"

        # check item to update exists
        _res = self.db_cursor.execute(
            f"SELECT {dict[table_name]['pk']} FROM {table_name};").fetchall()
        _existing_ids = map(lambda x: str(x[0]), _res)
        if data[0] not in _existing_ids:
            return f"{data[0]} does not exist in {table_name}"

        # convert arg to dictionary
        attributes = self.db_cursor.execute(
            f"PRAGMA table_info({table_name})").fetchall()
        for arg in data[1:]:
            _split = arg.strip().split("=")
            if len(_split) != 2:
                return "Invalid input: separate attribute name and new attribute value by '='"

            # check if attr is PK
            if _split[0] == dict[table_name]['pk']:
                return "Invalid input: cannot update primary key"
            # check if attr exists
            try:
                attr_idx = list(map(lambda x: x[1], attributes)).index(
                    _split[0])
            except:
                return f"Invalid input: {_split[0]} is not an attribute"

            if not self.validateField(_split[1], attributes[attr_idx], table_name):
                return f"Invalid data input for attribute: {_split[0]}"

    # validate fields according to their datatypes
    def validateField(self, data, attr, table_name):
        [_, name, datatype, notnull, default, pk] = attr
        # validate non-null fields
        if notnull:
            if data == None or data == "":
                print_error(f"{name} cannot be null")
                return False
        else:
            if data == None or data == "":
                return True

        # validate PK field
        if pk:
            _res = self.db_cursor.execute(
                f"SELECT {name} FROM {table_name};").fetchall()
            _existing_ids = map(lambda x: str(x[0]), _res)
            if data in _existing_ids:
                print_error(
                    f"{name} is a primary key but the provided id is not unique")
                return False

        # validate foreign key fields
        fk_list = self.db_cursor.execute(
            f"PRAGMA foreign_key_list({table_name})").fetchall()
        fk_from_names = list(map(lambda x: x[3], fk_list))
        try:
            idx = fk_from_names.index(name)
            [_, _, to_table, _, fk_to_name, _, _, _] = fk_list[idx]
            fk_values = self.db_cursor.execute(
                f"SELECT {fk_to_name} FROM {to_table};").fetchall()
            fk_values = list(map(lambda x: str(x[0]), fk_values))
            if data not in fk_values:
                print_error(
                    f"{name} is a foreign key and the provided value does not exist in the linked table [{to_table}].\nCreate a new entry at the linked table first.")
                return False
        except:
            pass

        # validate date fields
        if default == "'2022-01-01'":
            # validate date
            if not re.search("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", data):
                print_error(
                    f"{name} should be in date format: YYYY-MM-DD")
                return False
        # validate integer fields
        elif datatype == "INTEGER":
            if not data.isdigit():
                print_error(f"{name} should be an integer")
                return False
        return True

    # get the name of the PK of the table
    def getPKName(self, table_name):
        attributes = self.db_cursor.execute(
            f"PRAGMA table_info({table_name});").fetchall()
        for attr in attributes:
            [_, name, _, _, _, pk] = attr
            if pk:
                return name

    # check if the speicified value exists as the specified table's PK (which should be unique)
    def validateValueInPK(self, value, table_name, pk_name):
        check = self.db_cursor.execute(
            f"SELECT {pk_name} FROM {table_name} WHERE {pk_name} = ?;", [value]).fetchone()
        if not check:
            return f"{value} is not an unique identifier in {table_name}"

    # helper function format and print tables
    def display_table(self, query, params={}):
        df = pd.read_sql_query(query, self.conn, params=params)
        if df.shape[0] == 0:
            print_table("Table is empty")
        else:
            print_table(tabulate(df, headers='keys', tablefmt='psql'))


Main().execute()
