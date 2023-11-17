# this deals with every sql query.

import sqlite3
from datetime import date
import users


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
        try:
            cursor.execute(query, input_row)
        except sqlite3.Error as error:
            raise ValueError('error: could not properly select from table\nquery: ' + query + '\nparams:' + str(input_row) + '\nerror: ' + str(error))
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

    # cols is a tuple listing columns you want from the table
    # where_conds is a set of WhereConds objects that specify the conditions
    def select_row_col(self, cols: list = None, where_conds: list = None):

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


class VariableTable(BaseTable):

    def __init__(self, conn: sqlite3.Connection):
        cols = ('variable_id', 'var_name', 'var_values')
        col_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25)', 'json')
        name = 'variables'
        primary_key = 'variable_id'
        super().__init__(conn, name, cols, col_types, primary_key)


class CategoryTable(BaseTable):
    def __init__(self, conn: sqlite3.Connection):
        cols = ('category_id', 'name', 'game_id')
        col_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25)', 'VARCHAR(25)')
        name = 'categories'
        primary_key = 'category_id'
        super().__init__(conn, name, cols, col_types, primary_key)


class UserTable(BaseTable):

    def __init__(self, conn: sqlite3.Connection):
        cols = ('user_id', 'user_name', 'pronouns', 'user_type', 'user_pfp')
        col_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25)', 'VARCHAR(25)', 'VARCHAR(25)', 'VARCHAR(25)')
        name = 'users'
        primary_key = 'user_id'
        super().__init__(conn, name, cols, col_types, primary_key)


class MasterTable(BaseTable):
    def __init__(self, conn: sqlite3.Connection):
        cols = (
            'run_id',
            'game_id',
            'game_name',
            'run_date',
            'player_info',
            'player_name',
            'rta',
            'igt',
            'category_id',
            'category_name',
            'variable_id',
            'variable_info',
            'verifier_info',
            'verifier_name',
            'verify_date',
            'status'
        )
        col_types = (
            'VARCHAR(25)',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'date',
            'players',
            'VARCHAR(25)',
            'REAL',
            'REAL',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'json',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'date',
            'VARCHAR(25)'
        )
        name = 'runs_master'
        primary_key = 'run_id'
        super().__init__(conn, name, cols, col_types, primary_key)


def drop_all_tables(conn: sqlite3.Connection):
    cursor = conn.cursor()
    names = cursor.execute('SELECT name FROM sqlite_master').fetchall()
    [cursor.execute(f'DROP TABLE {row_dict.get("name")}') for row_dict in names if not row_dict.get('name').startswith('sqlite')]
    conn.commit()
    return


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
def adapt_users(players_obj: users.Users) -> str:
    return repr(players_obj)


def convert_users(user_bytes) -> users.Users:
    user_str = user_bytes.decode('utf-8')
    return users.get_users_from_repr(user_str)
