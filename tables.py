# this deals with every sql query.

import sqlite3
from datetime import date
import players


# base class for all tables later, contains sql calling logic
class BaseTable:

    def __init__(self, conn: sqlite3.Connection, name: str, cols: tuple, col_types: tuple, primary_key: str = None):
        self.conn = conn
        self.COLS = cols
        self.COL_TYPES = col_types
        self.NAME = name
        self.PRIMARY_KEY = primary_key
        self.create_table()

    def __call__(self, query, input_row: tuple = tuple()):
        print(query)
        cursor = self.conn.cursor()
        cursor.execute(query, input_row)
        data = cursor.fetchall()
        cursor.close()
        self.conn.commit()
        return data

    def executemany(self, query, input_rows: list):
        print(query)
        cursor = self.conn.cursor()
        cursor.executemany(query, input_rows)
        data = cursor.fetchall()
        cursor.close()
        self.conn.commit()
        return data

    def create_table(self):
        cols = tuple(f'{col} {col_type}' for col, col_type in zip(self.COLS, self.COL_TYPES))
        query = f'''CREATE TABLE IF NOT EXISTS {self.NAME} ({', '.join(cols)})'''
        return self(query)

    def insert_single_row(self, row: tuple):
        if len(row) != len(self.COLS):
            raise ValueError('error, invalid row')
        query = f'''
        INSERT INTO {self.NAME} {self.COLS}
        VALUES ({', '.join(['?' for _ in self.COLS])}) RETURNING *'''
        return self(query, row)

    def insert_multiple_runs(self, rows: list):
        query = f'''
        INSERT INTO {self.NAME} {self.COLS} 
        VALUES ({', '.join(['?' for _ in self.COLS])}) RETURNING *'''
        return self.executemany(query, rows)

    def select_cols(self, *args):
        # check if any of the kwarg keys are not valid columns
        if set(args) - set(self.COLS):
            raise ValueError('error: keyword not found in table')

        query = f'''SELECT {', '.join(args)} FROM {self.NAME}'''
        return self(query)

    # args will be raw WhereCond objects
    def select_row_col(self, cols=(), where_conds=()):

        invalid_args = (set(arg.col for arg in where_conds) - set(self.COLS))
        invalid_args.update(set(cols) - set(self.COLS))

        if invalid_args:
            raise ValueError('error: keyword not found in table')

        if not cols:
            cols = ('*',)

        conditions = [where_cond() for where_cond in where_conds]
        query = f'''SELECT {', '.join(cols)} FROM {self.NAME}'''
        if conditions:
            query += f''' WHERE {' AND '.join(conditions)}'''

        params = tuple(arg.value for arg in where_conds)
        return self(query, params)

    # kwargs will have to be in self.TABLE_COLS. the only condition that won't be updated about a run is its id.
    # if another kwarg is set, it will be updated
    def update_row(self, primary_key: str, **kwargs):
        if set(kwargs) - set(self.COLS):
            raise ValueError('error: keyword not found in table')

        keys = [f'{key} = ?' for key in kwargs]
        query = f'''UPDATE {self.NAME} SET {', '.join(keys)} WHERE {self.PRIMARY_KEY} = ? RETURNING *'''
        params = tuple(kwargs.values()) + (primary_key,)
        return self(query, params)

    def delete_row(self, run_id: str):
        query = f'''DELETE FROM {self.NAME} WHERE run_id = ? RETURNING *'''
        return self(query, (run_id,))

    def clear_table(self):
        return self(f'''DELETE FROM {self.NAME}''')

    def resync_table(self, new_rows: list):
        self.clear_table()
        self.insert_multiple_runs(new_rows)
        return True


"""
every run is in this table. the table will look like this:
run_id - game_id - player - date - rta - igt - category - variable - verifier - verify_date
run_id is the id of the run (also the primary key)
game_id is the id of the game the run was done in
runner is the runners id
date is the date stored in ISO8601 format
rta is the real time of the run (for every run)
igt is the in game time of the run (for runs that dont have an igt, the rta is used)
category is the id of the category
variable is the id's of variables - a serialized dictionary (pretty much a json).
verifier is the verifiers id (if not verified, NULL)
verify_date is the date that the run was verified in ISO8601 (if not verified NULL)
the keys are the ids of choices types, the values are ids of the specific choices
"""


class RunTable(BaseTable):

    def __init__(self, conn: sqlite3.Connection):
        table_cols = ('run_id', 'game_id', 'player', 'date', 'rta', 'igt', 'category', 'variable', 'verifier', 'verify_date')
        table_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25)', 'players', 'date', 'REAL', 'REAL', 'VARCHAR(25)', 'json', 'VARCHAR(25)', 'date')
        name = 'runs'
        primary_key = 'run_id'
        super().__init__(conn, name, table_cols, table_types, primary_key)


class VariableTable(BaseTable):

    def __init__(self, conn: sqlite3.Connection):
        cols = ('variable_id', 'var_name', 'var_values')
        col_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25)', 'json')
        name = 'variables'
        primary_key = 'variable_id'
        super().__init__(conn, name, cols, col_types, primary_key)


class CategoryTable(BaseTable):
    def __init__(self, conn: sqlite3.Connection):
        cols = ('category_id', 'name')
        col_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25)')
        name = 'categories'
        primary_key = 'category_id'
        super().__init__(conn, name, cols, col_types, primary_key)


class UserTable(BaseTable):

    def __init__(self, conn: sqlite3.Connection):
        cols = ('user_id', 'name', 'type')
        col_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25), VARCHAR(25)')
        name = 'users'
        primary_key = 'user_id'
        super().__init__(conn, name, cols, col_types, primary_key)


# i promise ill make this work by the end of this week, just not right now
def master_table(conn):
    query = ''''''
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    return data


# this is for sqlite3 connection to transform rows into dictionaries
def dict_factory(cursor: sqlite3.Cursor, row):
    cols = [col[0] for col in cursor.description]
    return {key: value for key, value in zip(cols, row)}


# these functions are adapters/converters for the formats i will be using.
# for dates
def adapt_date_iso(date_obj: date or None) -> str or None:
    return date_obj.isoformat() if date_obj else None


def convert_date_iso(date_iso) -> date or None:
    return date.fromisoformat(date_iso.decode('utf-8')) if date_iso else None


# for players
def adapt_players(players_obj: players.Players) -> str:
    return repr(players_obj)


def convert_players(players_bytes) -> players.Players:
    players_str = players_bytes.decode('utf-8')
    return players.get_players_from_repr(players_str)
