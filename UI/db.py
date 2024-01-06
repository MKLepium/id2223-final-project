from sshtunnel import SSHTunnelForwarder
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os

load_dotenv()

def connect_to_sshtunnel():
    server = SSHTunnelForwarder(
        ('88.99.215.78', 2224),
        ssh_username=os.getenv("SSH_USERNAME"),
        ssh_pkey=os.getenv("SSH_PKEY"),
        remote_bind_address=('127.0.0.1', 5432),
        local_bind_address=('127.0.0.1', 5432)
    )

    server.start()

    print(server.local_bind_port)  # show assigned local port
    # work with `SECRET SERVICE` through `server.local_bind_port`.
    assert server.is_active
    return server


# connect to database bus_db with user postgres and password db_password
def connect_to_db(server):
    conn = psycopg2.connect(
        database=os.getenv("DB_DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=server.local_bind_host,
        port=server.local_bind_port
    )
    return conn

class DB:
    def __init__(self, local: bool=False) -> None:
        self.server = None
        self.conn = None
        self.local = local

    def init(self):
        if self.local:
            self.conn = psycopg2.connect(
                database=os.getenv("DB_DATABASE"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host="localhost",
                port=5432
            )
        else:
            self.server = connect_to_sshtunnel()
            self.conn = connect_to_db(self.server)

    def close(self):
        if self.conn:
            self.conn.close()
        if self.server:
            self.server.stop()

    def query(self, query, params = ()):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # check available databases
            try:
                cur.execute(query, params)
                self.conn.commit()
                return cur.fetchall()
            except Exception as e:
                print(e)
                print("ROLLBACK")
                cur.execute("ROLLBACK")

    def read_day(self, date):
        # get query from read_day.sql
        with open("./sql/read_day.sql", "r") as f:
            q = f.read()
            res = self.query(q, (date + " 00:00:00", date + " 23:59:59"))
            return res

    def insert_values(self, values):
        print("Inserting values", values)
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO bus_data_schema.delay_wasp VALUES %s
            """, values)
            print("Inserted values", values, cur.statusmessage)
            cur.execute("COMMIT")
        return cur.statusmessage
        print("Failed to insert values", values)


class DBManager():
    def __init__(self):
        self.db = None
          
    def __enter__(self):
        
        self.db = DB(local=os.environ.get("LOCAL", False))
        self.db.init()
        return self.db
      
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.db.close()


if __name__ == "__main__":
    with DBManager() as db:
        # res = db.query("SELECT datname FROM pg_database;")
        res = db.insert_values([("2021-09-01", 'Fakefer2', 1, 2, 3, "day")])
    print(res)