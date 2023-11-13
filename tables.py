# this deals with every sql query.

import sqlite3
from datetime import date
import players


# base class for all tables later, contains sql calling logic
class SrcTable:
    def __init__(self, conn):
        self.conn = conn

    def __call__(self, query, input_row: tuple = tuple()):
        cursor = self.conn.cursor()
        cursor.execute(query, input_row)
        data = cursor.fetchall()
        cursor.close()
        self.conn.commit()
        return data

    def executemany(self, query, input_rows: list):
        cursor = self.conn.cursor()
        cursor.executemany(query, input_rows)
        data = cursor.fetchall()
        cursor.close()
        self.conn.commit()
        return data


# represents all game's runs that are in config
class RunTable(SrcTable):
    TABLE_COLS = ('run_id', 'game_id', 'player', 'date', 'rta', 'igt', 'category', 'variable', 'verifier', 'verify_date')
    CHECK_NULL = ('check_null', 'check_not_null')
    VALID_KWARGS = TABLE_COLS + CHECK_NULL

    def __init__(self, conn: sqlite3.Connection):
        super().__init__(conn)
        self.create_game_table()

    def __repr__(self):
        query = '''SELECT * FROM runs'''
        return str(self(query))

    """
    each game has a table. the table will look like this:
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

    def create_game_table(self):
        query = f'''
        CREATE TABLE IF NOT EXISTS runs(
        run_id VARCHAR(25) PRIMARY KEY, 
        game_id VARCHAR(25),
        player players, 
        date date, 
        rta REAL, 
        igt REAL, 
        category VARCHAR(25), 
        variable json, 
        verifier VARCHAR(25), 
        verify_date date
        )'''
        return self(query)

    def insert_single_run(self, row: tuple):
        query = f'''
        INSERT INTO runs
        (run_id, game_id, player, date, rta, igt, category, variable, verifier, verify_date) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING *'''
        return self(query, row)

    def insert_multiple_runs(self, rows: list):
        query = f'''
        INSERT INTO runs
        (run_id, game_id, player, date, rta, igt, category, variable, verifier, verify_date) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING *'''
        return self.executemany(query, rows)

    # allows for debugging of multiple run inserts by executing run by run
    def insert_multiple_runs_debug(self, rows: list):
        cursor = self.conn.cursor()
        query = f'''
        INSERT INTO runs
        (run_id, game_id, player, date, rta, igt, category, variable, verifier, verify_date) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING *'''
        ids = []
        for i, run in enumerate(rows):
            if run[0] in ids:
                print(f"duplicate found, {run[0]} at index {i} and index {ids.index(run[0])}")
            ids.append(run[0])
        for run in rows:
            cursor.execute(query, run)

        inputted_rows = cursor.fetchall()
        cursor.close()
        self.conn.commit()
        return inputted_rows

    # all arguments passed in should be in self.TABLE_COLS
    def select_rows(self, **kwargs):
        # check if any of the kwarg keys are not valid columns
        if set(kwargs) - set(self.TABLE_COLS):
            raise ValueError('error: keyword not found in table')

        keys = [f'{key} = ?' for key in kwargs]
        query = f'''SELECT * FROM runs'''
        if kwargs:
            query += f''' WHERE {' AND '.join(keys)}'''
        params = tuple(kwargs.values()) if kwargs else tuple()
        return self(query, params)

    def select_cols(self, *args):
        # check if any of the kwarg keys are not valid columns
        if set(args) - set(self.TABLE_COLS):
            raise ValueError('error: keyword not found in table')

        query = f'''SELECT {', '.join(args)} FROM runs'''
        return self(query)

    def select_row_col(self, *args, **kwargs):

        invalid_args = set(args) - set(self.TABLE_COLS)
        invalid_args.update(set(kwargs) - set(self.VALID_KWARGS))
        invalid_args.update(set(kwargs.get('check_null', [])) - set(self.TABLE_COLS))
        invalid_args.update(set(kwargs.get('check_not_null', [])) - set(self.TABLE_COLS))

        if invalid_args:
            raise ValueError('error: keyword not found in table')

        conditions = []
        conditions.extend([f'{key} = ?' for key in kwargs if kwargs[key] and key not in self.CHECK_NULL])
        conditions.extend([f'{key} IS NULL' for key in kwargs.get('check_null', ())])
        conditions.extend([f'{key} IS NOT NULL' for key in kwargs.get('check_not_null', ())])

        query = f'''SELECT {', '.join(args)} FROM runs'''

        if conditions:
            query += f''' WHERE {' AND '.join(conditions)}'''

        params = tuple(value for key, value in kwargs.items() if value and key not in self.CHECK_NULL)
        return self(query, params)

    # kwargs will have to be in self.TABLE_COLS. the only condition that won't be updated about a run is its id.
    # if another kwarg is set, it will be updated
    def update_row(self, run_id: str, **kwargs):
        if set(kwargs) - set(self.TABLE_COLS):
            raise ValueError('error: keyword not found in table')

        keys = [f'{key} = ?' for key in kwargs]
        query = f'''UPDATE runs SET {', '.join(keys)} WHERE run_id = ? RETURNING *'''
        params = tuple(kwargs.values()) + (run_id,)
        return self(query, params)

    def delete_row(self, run_id: str):
        query = f'''DELETE FROM runs WHERE run_id = ? RETURNING *'''
        return self(query, (run_id,))

    def clear_table(self):
        return self('''DELETE FROM runs RETURNING *''')

    # DONT EVER DO THIS ITS A TERRIBLE IDEA
    def execute(self, statement):
        return self(statement)

    def resync_table(self, new_rows: list):
        self.clear_table()
        self.insert_multiple_runs(new_rows)
        return True


class VariableTable(SrcTable):
    TABLE_COLS = ('variable_id', 'var_name', 'var_values')
    CHECK_NULL = ('check_null', 'check_not_null')
    VALID_KWARGS = TABLE_COLS + CHECK_NULL

    def __init__(self, conn):
        super().__init__(conn)
        self.create_table()

    def __repr__(self):
        query = '''SELECT * FROM variables'''
        return str(self(query))

    def create_table(self):
        query = ('''CREATE TABLE IF NOT EXISTS variables(
        variable_id VARCHAR(25) PRIMARY KEY,
        var_name VARCHAR(25),
        var_values json)
        ''')
        self(query)
        return True

    def insert_single_run(self, row: tuple):
        query = f'''
        INSERT INTO variables (variable_id, var_name, var_values) 
        VALUES (?, ?, ?) RETURNING *'''
        return self(query, row)

    def insert_multiple_runs(self, rows: list):
        query = f'''
        INSERT INTO variables (variable_id, var_name, var_values) 
        VALUES (?, ?, ?) RETURNING *'''
        return self.executemany(query, rows)

    # all arguments passed in should be in self.TABLE_COLS
    def select_rows(self, **kwargs):
        # check if any of the kwarg keys are not valid columns
        if set(kwargs) - set(self.TABLE_COLS):
            raise ValueError('error: keyword not found in table')

        keys = [f'{key} = ?' for key in kwargs]
        query = f'''SELECT * FROM variables'''
        if kwargs:
            query += f''' WHERE {' AND '.join(keys)}'''
        params = tuple(kwargs.values())
        return self(query, params)

    def select_cols(self, *args):
        # check if any of the kwarg keys are not valid columns
        if set(args) - set(self.TABLE_COLS):
            raise ValueError('error: keyword not found in table')

        return self(f'''SELECT {', '.join(args)} FROM variables''')

    def select_row_col(self, *args, **kwargs):

        invalid_args = set(args) - set(self.TABLE_COLS)
        invalid_args.update(set(kwargs) - set(self.VALID_KWARGS))
        invalid_args.update(set(kwargs.get('check_null', [])) - set(self.TABLE_COLS))
        invalid_args.update(set(kwargs.get('check_not_null', [])) - set(self.TABLE_COLS))

        if invalid_args:
            raise ValueError('error: keyword not found in table')

        conditions = []
        conditions.extend([f'{key} = ?' for key in kwargs if kwargs[key] and key not in self.CHECK_NULL])
        conditions.extend([f'{key} IS NULL' for key in kwargs.get('check_null', ())])
        conditions.extend([f'{key} IS NOT NULL' for key in kwargs.get('check_not_null', ())])

        query = f'''SELECT {', '.join(args)} FROM variables'''

        if conditions:
            query += f''' WHERE {' AND '.join(conditions)}'''

        params = tuple(value for key, value in kwargs.items() if value and key not in self.CHECK_NULL)
        return self(query, params)

    def delete_row(self, run_id: str):
        query = f'''DELETE FROM runs WHERE run_id = ? RETURNING *'''
        return self(query, (run_id,))

    def clear_table(self):

        return self('''DELETE FROM runs RETURNING *''')

    # DONT EVER DO THIS ITS A TERRIBLE IDEA
    def execute(self, statement):
        return self(statement)

    def resync_table(self, new_rows: list):
        self.clear_table()
        self.insert_multiple_runs(new_rows)
        return True


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
    return date.fromisoformat(date_iso) if date_iso else None


# for players
def adapt_players(players_obj: players.Players) -> str:
    return repr(players_obj)


def convert_players(players_str) -> players.Players:
    return players.get_players_from_repr(players_str)
