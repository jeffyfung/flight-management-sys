import sqlite3
import traceback
import re
from termcolor import colored, cprint


def print_status(str):
    cprint(str, "cyan")


def print_error(str):
    cprint(str, "red")


def print_table(str):
    cprint(str, "green")


def format_prompt(str):
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
            "alter": self.alter,
            "stat": self.stat
        }
        print_status("Starting airline database control CLI application...")
        # initialize databases
        self.initDB()

    def initDB(self):
        try:
            # initialize connection with sqlite
            self.conn = sqlite3.connect(self.db_name)
            print_status(f"Successfully connected with DB: {self.db_name}")
            self.db_cursor = self.conn.cursor()
            self.db_cursor.execute("PRAGMA foreign_keys = ON")

            # initialize tables
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

            if not self.db_cursor.execute("SELECT COUNT(1) FROM pilot;").fetchone()[0]:
                init_pilot_query = "INSERT INTO pilot (staff_id, last_name, first_name) VALUES (:staff_id, :last_name, :first_name);"
                pilot_data = [
                    {"staff_id": "1", "last_name": "beckham", "first_name": "david"},
                    {"staff_id": "2", "last_name": "kane", "first_name": "harry"}
                ]
                self.db_cursor.executemany(init_pilot_query, pilot_data)

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

    def insert(self):
        try:
            insert_dict = {
                "flight": {
                    "prompt": "Input data in the following format: flight_id, start_airport, destination_airport, departure_date (YYYY-MM-DD), aircraft_id, pilot_1, pilot_2\nNote:\n\t- flight_id must be unique\n\t- Only pilot_2 is optional. If you skip this field, still input a comma to separate the field with other fields\n",
                    "query": "INSERT INTO flight (flight_id, start_airport, destination_airport, departure_date, aircraft_id, pilot_1, pilot_2) VALUES (?, ?, ?, ?, ?, ?, ?)"
                },
                "pilot": {
                    "prompt": "Input data in the following format: staff_id, last_name, first_name\nNote:\n\t- staff_id must be unique\n\t- all fields are compulsory\n",
                    "query": "INSERT INTO pilot (staff_id, last_name, first_name) VALUES (?, ?, ?)"
                },
                "aircraft": {
                    "prompt": "Input data in the following format: staff_id, last_name, first_name\nNote:\n\t- staff_id must be unique\n\t- all fields are compulsory\n",
                    "query": "INSERT INTO aircraft (aircraft_id, age, capacity) VALUES (?, ?, ?)"
                }
            }

            prompt = format_prompt(
                f"Which table to insert to? ({' '.join(list(insert_dict.keys()))}): ")
            option = input(prompt).strip()
            if not option or not (option in list(insert_dict.keys())):
                print_error(
                    f"Invalid input; Only accept: {' '.join(list(insert_dict.keys()))}")
                return

            data = [field.strip() for field in input(
                format_prompt(insert_dict[option]["prompt"])).split(",")]

            if error := self.validateData(data, option):
                print_error(error)
                return

            self.db_cursor.execute(insert_dict[option]["query"], data)
            self.conn.commit()
            print_status('Data successfully inserted.')
        except Exception as e:
            print_error(e)
            print(traceback.format_exc())

    def update(self):
        try:
            update_dict = {
                "flight": {
                    "prompt": "Update flight data in the following format: flight_id, attribute_1=attribute_1_new, attribute_2=attribute_2_new...\nNotes:\n\t- e.g. BA123, start_airport=BLN\n\t- flight_id is not editable\n",
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

            prompt = format_prompt(
                f"Which table to update? ({' '.join(list(update_dict.keys()))}): ")
            option = input(prompt).strip()
            if not option or not (option in list(update_dict.keys())):
                print_error(
                    f"Invalid input; Only accept: {' '.join(list(update_dict.keys()))}")
                return

            data = [field.strip() for field in input(
                format_prompt(update_dict[option]["prompt"])).split(",")]

            if error := self.validateUpdateData(data, update_dict, option):
                print_error(error)
                return

            conds = []
            params = []
            for arg in data[1:]:
                _split = arg.split("=")
                conds.append(_split[0].strip() + "=?")
                params.append(_split[1].strip())
            _query = update_dict[option]["query"].replace(
                "conds_placeholder", ",".join(conds))
            params.append(data[0])

            self.db_cursor.execute(_query, params)
            self.conn.commit()
            print_status(f'Successfully updated {option} {data[0]}.')
        except Exception as e:
            print_error(e)
            print(traceback.format_exc())

    def delete(self):
        try:
            prompt = format_prompt(
                "Input the flight_id of the flight you wish to delete:\n")
            flight_id = input(prompt).strip()

            if not self.checkItemExistInTable(flight_id, "flight_id", "flight"):
                print_error("Invalid flight_id")
                return

            _query = "DELETE FROM flight WHERE flight_id = :flight_id"
            self.db_cursor.execute(_query, {"flight_id": flight_id})
            self.conn.commit()
            print_status(
                f'Successfully delete flight {flight_id}. Here is the latest view of flight:')
            self.view("flight")
        except:
            print(traceback.format_exc())

    def search(self):
        raise NotImplementedError()

    def alter(self):
        raise NotImplementedError()

    def view(self, option=False):
        try:
            prompt = format_prompt("Input flight / pilot / aircraft / all:\n")
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

            if not option:
                option = input(prompt).strip()

            if _query := query_dict.get(option, False):
                self.display_table(_query)
            else:
                print_error(
                    "Invalid input; Only accept: flight pilot aircraft all")

        except Exception as e:
            print_error(e)
            print(traceback.format_exc())

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

            if not option or not (option in list(stat_dict.keys())):
                print_error("Invalid input")
                return

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

    def showManuel(self):
        raise NotImplementedError()

    def execute(self):
        while cmd := input(format_prompt(f"Enter a command ({self.exit_cmd} to quit): ")).strip():
            if cmd == self.exit_cmd:
                self.db_cursor.close()
                self.conn.close()
                break
            if op := self.cmd_types.get(cmd):
                op()
            else:
                print_error("Invalid input; Only accepts the following commands: " +
                            " ".join(self.cmd_types.keys()))

    def validateData(self, data, table_name):
        attributes = self.db_cursor.execute(
            f"PRAGMA table_info({table_name})").fetchall()
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

    def validateField(self, data, attr, table_name):
        # TODO: foreign key validation
        [_, name, datatype, notnull, default, pk] = attr
        if notnull:
            if data == "":
                print_error(f"{name} cannot be null")
                return False
        else:
            if data == "":
                return True

        if pk:
            _res = self.db_cursor.execute(
                f"SELECT {name} FROM {table_name};").fetchall()
            _existing_ids = map(lambda x: str(x[0]), _res)
            if data in _existing_ids:
                print_error(
                    f"{name} is a primary key but the provided id is not unique")
                return False

        # validate foreign key
        fk_list = self.db_cursor.execute(
            f"PRAGMA foreign_key_list({table_name})").fetchall()
        fk_from_names = list(map(lambda x: x[3], fk_list))
        try:
            idx = fk_from_names.index(name)
            [_, _, to_table, _, fk_to_name, _, _, _] = fk_list[idx]
            print(to_table, fk_to_name)
            fk_values = self.db_cursor.execute(
                f"SELECT {fk_to_name} FROM {to_table};").fetchall()
            fk_values = list(map(lambda x: str(x[0]), fk_values))
            if data not in fk_values:
                print_error(
                    f"{name} is a foreign key and the provided value does not exist in the linked table [{to_table}].\nCreate a new entry at the linked table first.")
                return False
        except:
            pass

        if default == "'2022-01-01'":
            # validate date
            if not re.search("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", data):
                print_error(
                    f"{name} should be in date format: YYYY-MM-DD")
                return False
        elif datatype == "INTEGER":
            if not data.isdigit():
                print_error(f"{name} should be an integer")
                return False
        return True

    def checkItemExistInTable(self, value, attribute, table):
        _query = f"SELECT {attribute} FROM {table};"
        return (value, ) in self.db_cursor.execute(_query).fetchall()

    def display_table(self, query, params={}):
        res = self.db_cursor.execute(query, params)
        headers = map(lambda x: str(x[0]), self.db_cursor.description)
        print_table("  ".join(headers))
        for row in res:
            print_table("  ".join(map(lambda x: str(x), row)))
        # TODO: get a nicer presentation


Main().execute()
