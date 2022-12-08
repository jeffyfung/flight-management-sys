import sqlite3
import traceback
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
        self.tableHeaders = {}
        for name in self.table_names:
            self.tableHeaders[name] = self.getHeaders(name)

    def getHeaders(self, table):
        query = "SELECT name FROM PRAGMA_TABLE_INFO(?)"
        res = self.db_cursor.execute(query, (table, )).fetchall()
        return [row[0] for row in res]

    def initDB(self):
        try:
            # initialize connection with sqlite
            self.conn = sqlite3.connect(self.db_name)
            print_status(f"Successfully connected with DB: {self.db_name}")
            self.db_cursor = self.conn.cursor()

            # initialize tables
            self.db_cursor.execute("\
                CREATE TABLE IF NOT EXISTS aircraft (\
                    aircraft_id TEXT PRIMARY KEY,\
                    age INTEGER NOT NULL,\
                    capacity INTEGER NOT NULL\
                );")
            print_status("Successfully connected with table: aircraft")

            self.db_cursor.execute("\
                CREATE TABLE IF NOT EXISTS flight (\
                    flight_id TEXT PRIMARY KEY,\
                    start_airport TEXT NOT NULL,\
                    destination_airport TEXT NOT NULL,\
                    departure_date TEXT NOT NULL,\
                    aircraft_id TEXT NOT NULL,\
                    pilot_1 TEXT NOT NULL,\
                    pilot_2 TEXT NOT NULL\
                );")
            print_status("Successfully connected with table: flight")

            self.db_cursor.execute("\
                CREATE TABLE IF NOT EXISTS pilot (\
                    staff_id TEXT PRIMARY KEY,\
                    last_name TEXT NOT NULL,\
                    first_name TEXT NOT NULL\
                );")
            print_status("Successfully connected with table: pilot")

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
            prompt = format_prompt(
                "Input flight data in the following format:\nflight_id, start_airport, destination_airport, departure_date (YYYY-MM-DD), aircraft_id, pilot_1, pilot_2.\nNote: flight_id must be a unique string (e.g. BA 123)\nNote: Only pilot_2 is optional\n")
            data = [field.strip() for field in input(prompt).split(",")]
            if len(data) != 6 and len(data) != 7:
                print_error("Invalid input")
                return
            params = {
                "flight_id": data[0],
                "start_airport": data[1],
                "destination_airport": data[2],
                "departure_date": data[3],
                "aircraft_id": data[4],
                "pilot_1": data[5],
                "pilot_2": data[6],
            }

            # check whether the aircraft and pilots exist
            # if len(data) == 7:
            #     params["pilot_2"] = data[6]

            if not self.checkItemExistInTable(params["aircraft_id"], "aircraft_id", "aircraft"):
                print_error(f"aircraft_id does not exist")
                return

            if not self.checkItemExistInTable(params["pilot_1"], "staff_id", "pilot"):
                print_error(f"pilot_1 does not exist")
                return

            if not self.checkItemExistInTable(params["pilot_2"], "staff_id", "pilot"):
                print_error(f"pilot_2 does not exist")
                return

            # if len(params) == 7:
            _query = "INSERT INTO flight (flight_id, start_airport, destination_airport, departure_date, aircraft_id, pilot_1, pilot_2) VALUES (:flight_id, :start_airport, :destination_airport, :departure_date, :aircraft_id, :pilot_1, :pilot_2);"
            # else:
            #     _query = "INSERT INTO flight (flight_id, start_airport, destination_airport, departure_date, aircraft_id, pilot_1) VALUES (:flight_id, :start_airport, :destination_airport, :departure_date, :aircraft_id, :pilot_1);"

            self.db_cursor.execute(_query, params)
            self.conn.commit()
            print_status('Data successfully inserted.')
        except Exception as e:
            print_error(e)
            print(traceback.format_exc())

    def update(self):
        try:
            prompt = format_prompt(
                "Update flight data in the following format:\nflight_id, attribute_1=attribute_1_new, attribute_2=attribute_2_new...\ne.g. BA123, start_airport=BLN\nNote: flight_id is not editable.\n")
            data = [field.strip() for field in input(prompt).split(",")]

            if len(data) < 2:
                print_error("Invalid input")
                return

            params = {"flight_id": data[0]}
            for attr_str in data[1:]:
                attr = attr_str.split("=")
                if len(attr) != 2:
                    print_error(
                        "Invalid input: attribute key and value must be separated by =")
                    return
                if attr[0] not in self.flightTableHeader:
                    print_error("Invalid input: attribute key is invalid")
                    return
                params[attr[0]] = attr[1]

            conds = ','.join(
                f"{key}=:{key}" for key in params if key != "flight_id")
            _query = f"UPDATE flight SET {conds} WHERE flight_id=:flight_id"
            self.db_cursor.execute(_query, params)
            self.conn.commit()
            print_status(f'Successfully updated flight {data[0]}.')
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
            self.view_flight()
        except:
            print(traceback.format_exc())

    def search(self):
        raise NotImplementedError()

    def alter(self):
        raise NotImplementedError()

    def view(self):
        try:
            prompt = format_prompt(
                "Input flight / pilot / aircraft / all:\n")

            subFunc = {
                "flight": self.view_flight,
                "pilot": self.view_pilot,
                "aircraft": self.view_aircraft,
                "all": self.view_all
            }
            subFunc.get(input(prompt).strip(), self.view_invalid)()

        except:
            print(traceback.format_exc())

    def view_flight(self):
        _h = self.tableHeaders["flight"]
        print_table(
            f"{_h[0]:15} {_h[1]:15} {_h[2]:20} {_h[3]:20} {_h[4]:15} {_h[5]:10} {_h[6]}")
        for row in self.db_cursor.execute("SELECT * FROM flight"):
            print_table(
                f"{row[0]:15} {row[1]:15} {row[2]:20} {row[3]:20} {row[4]:15} {row[5]:10} {row[6]}")

    def view_pilot(self):
        _h = self.tableHeaders["pilot"]
        print_table(f"{_h[0]:15} {_h[1]:20} {_h[2]}")
        for row in self.db_cursor.execute("SELECT * FROM pilot"):
            print_table(f"{row[0]:15} {row[1]:20} {row[2]}")

    def view_aircraft(self):
        _h = self.tableHeaders["aircraft"]
        print_table(f"{_h[0]:15} {_h[1]:20} {_h[2]}")
        for row in self.db_cursor.execute("SELECT * FROM aircraft"):
            print_table(f"{row[0]:15} {str(row[1]):20} {row[2]}")

    def view_all(self):
        # TODO: adjust for changable column names etc
        _query = "SELECT flight_id, start_airport, destination_airport, departure_date, a.aircraft_id, a.age AS aircraft_age, a.capacity AS aircraft_capacity, pilot_1, p1.first_name || ' ' || p1.last_name AS pilot1_name, pilot_2, p2.first_name || ' ' || p2.last_name AS pilot2_name\
            FROM flight \
            INNER JOIN aircraft AS a ON flight.aircraft_id = a.aircraft_id \
            INNER JOIN pilot AS p1 ON flight.pilot_1 = p1.staff_id \
            INNER JOIN pilot AS p2 ON flight.pilot_2 = p2.staff_id"
        # TODO: to find a better way to print table
        _h = ["flight_id", "start_airport", "destination_airport", "departure_date", "aircraft_id",
              "aircraft_age (years)", "aircraft_capacity", "pilot1_id", "pilot1_name", "pilot2_id", "pilot2_name"]
        print_table(
            f"{_h[0]:15} {_h[1]:15} {_h[2]:20} {_h[3]:20} {_h[4]:15} {_h[5]:10} {_h[6]:15} {_h[7]:15} {_h[8]:15} {_h[9]:15} {_h[10]:15}")
        for row in self.db_cursor.execute(_query):
            print_table(
                f"{row[0]:15} {row[1]:15} {row[2]:20} {row[3]:20} {row[4]:15} {row[5]:10} {row[6]:15} {row[7]:15} {row[8]:15} {row[9]:15} {row[10]:15}")

    def view_invalid(self):
        print_error(
            "Invalid input for view - only flight / pilot / aircraft / all is accepted")

    def stat(self):
        try:
            stat_dict = {
                1: {
                    "description": "total number of flight (group by destination)",
                    "query": "SELECT destination_airport, COUNT(*) AS total_flight FROM flight GROUP BY destination_airport",
                },
                2: {
                    "description": "total number of flight (group by month)",
                    "query": "SELECT month, COUNT(*) AS total_flight FROM\
                        (SELECT substr(departure_date, 6, 2) AS month FROM flight) GROUP BY month ORDER BY month",
                },
                3: {
                    "description": "totaL number of flight (group by month and destination)",
                    "query": "SELECT month, destination_airport, COUNT(*) AS total_flight FROM\
                        (SELECT substr(departure_date, 6, 2) AS month, destination_airport FROM flight)\
                        GROUP BY month, destination_airport\
                        ORDER BY month",
                },
                4: {
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
                        ON pilot_id = staff_id"
                },
                5: {
                    "description": "average aircraft age",
                    "query": "SELECT AVG(age) AS average_age FROM aircraft"
                },
            }

            prompt = format_prompt(f"""\
                \nSelect the statistics you wish to view (input 1-5):\
                \n\t1. {stat_dict[1]["description"]}\
                \n\t2. {stat_dict[2]["description"]}\
                \n\t3. {stat_dict[3]["description"]}\
                \n\t3. {stat_dict[4]["description"]}\
                \n\t5. {stat_dict[5]["description"]}\
            """)
            print(prompt)

            try:
                option = int(input().strip())
                if not option or not option in list(range(1, 10)):
                    print_error("Invalid input. Input a number from 1-9")
                    return
            except:
                print_error("Invalid input. Input a number from 1-9")
                return

            if option == 5:
                avg_aircraft_age = self.db_cursor.execute(
                    stat_dict[option]["query"]).fetchone()[0]
                print_table(f"Average aircraft age is {avg_aircraft_age}")
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
                break
            if op := self.cmd_types.get(cmd):
                op()
            else:
                print_error("Invalid input; Only accepts the following commands: " +
                            " ".join(self.cmd_types.keys()))

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

    # def _getHeader(self):
    #     return list(map(lambda x: x[0], self.db_cursor.description))


Main().execute()
