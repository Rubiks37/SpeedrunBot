# this deals with every sql query.

import sqlite3
from datetime import date
import users
from where import WhereCond


# base class for all tables later, contains sql calling logic
class BaseTable:

    def __init__(self, conn: sqlite3.Connection, name: str, cols: tuple, col_types: tuple, primary_key: str = None):
        self.conn = conn
        self.COLS = cols
        self.COL_TYPES = col_types
        self.NAME = name
        self.PRIMARY_KEY = primary_key
        self.create_table()

    def __call__(self, query, input_row: tuple = tuple(), error_handle_graceful: bool = False):
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, input_row)
        except sqlite3.Error as error:
            if error_handle_graceful:
                print('ignoring exception\nquery: ' + query + '\nparams:' + str(input_row) + '\nerror: ' + str(error))
            else:
                raise ValueError('error: could not properly select from table\nquery: ' + query + '\nparams:' + str(input_row) + '\nerror: ' + str(error))
        data = cursor.fetchall()
        cursor.close()
        self.conn.commit()
        return data

    def executemany(self, query, input_rows: list):
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, input_rows)
        except sqlite3.Error as error:
            raise ValueError('error: could not properly select from table\nquery:', query, '\nerror:', str(error))
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
    def select_row_col(self, cols: list = None, where_conds: list = None, append: str = None):

        if not cols:
            cols = ['*']

        conditions = [where_cond() for where_cond in where_conds] if where_conds else []
        query = f'''SELECT {', '.join(cols)} FROM {self.NAME}'''
        if conditions:
            query += f''' WHERE {' AND '.join(conditions)}'''
        if append:
            query += append

        params = tuple(arg.value for arg in where_conds) if where_conds else ()
        return self(query, params)

    # so this function will take in a current keyword and search the columns for this keyword and return matches
    def search_table(self, current: str, cols: list = None):
        if not cols:
            cols = ['*']
        where_conditions = [f'{col} LIKE ?' for col in self.COLS]
        query = f'''SELECT {', '.join(cols)} FROM {self.NAME} WHERE ''' + ' OR '.join(where_conditions)
        params = (f'%{current}%',) * len(self.COLS)
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

    def drop_table(self):
        return self(f'''DROP TABLE IF EXISTS {self.NAME}''')

    def resync_table(self, new_rows: list):
        self.drop_table()
        self.create_table()
        self.insert_multiple_runs(new_rows)
        return True


class VariableTable(BaseTable):

    def __init__(self, conn: sqlite3.Connection):
        cols = ('variable_id', 'category_id', 'var_name', 'var_values')
        col_types = ('VARCHAR(25) PRIMARY KEY', 'VARCHAR(25)', 'VARCHAR(25)', 'json')
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
            'run_video',
            'comment',
            'category_id',
            'category_name',
            'variable_id',
            'variable_info',
            'verifier_info',
            'verifier_name',
            'verify_date',
            'status',
            'reason'
        )
        col_types = (
            'VARCHAR(25)',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'date',
            'users',
            'VARCHAR(25)',
            'REAL',
            'REAL',
            'VARCHAR(255)',
            'TEXT',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'VARCHAR(25)',
            'json',
            'users',
            'VARCHAR(25)',
            'date',
            'VARCHAR(25)',
            'TEXT'
        )
        name = 'runs_master'
        primary_key = 'run_id'
        super().__init__(conn, name, cols, col_types, primary_key)

    def get_embed_attributes_from_run_id(self, run_id, format_time_func):
        selected_cols = ['game_name', 'player_info', 'run_date', 'run_video', 'comment', 'rta', 'igt', 'category_name',
                         'variable_info', 'verifier_info', 'status', 'reason']
        run = next(iter(self.select_row_col(cols=selected_cols, where_conds=[WhereCond('run_id', '=', run_id)])),[])
        game_name = run.get('game_name')
        category_name = run.get('category_name')
        time = format_time_func(run.get('igt'))
        player_obj = run.get('player_info')
        players = f'''**Runners**: {', '.join([f"{player.get('user_name')} (*{player.get('pronouns')}*)" if player.get('pronouns') is not None else f"{player.get('user_name')}" for player in player_obj.users.values()])}'''
        variables_info = f'''**Subcategory**: {', '.join(run.get('variable_info').values())}'''
        run_date = f'''**Date of Run**: {run.get('run_date').isoformat()}''' if run.get('run_date') else None
        comment = f'''**Comment**: {run.get('comment')}\n''' if run.get('comment') else None
        verifier_obj = run.get('verifier_info')
        verifier_obj = next(iter(verifier_obj.users.values())) if verifier_obj else {}
        verifier = '**Verifier**: 'f"{verifier_obj.get('user_name')}" + f" (*{verifier_obj.get('pronouns')}*)" \
            if verifier_obj.get('pronouns') is not None else f"**Verifier**: {verifier_obj.get('user_name')}" \
            if verifier_obj.get('user_name') else None
        status = '**Status**: ' + run.get('status').capitalize() if run.get('status') != 'new' else 'Unverified'
        reason = '**Reason for Rejection**: ' + run.get('reason') if run.get('reason') else None

        title = f'''{game_name} {category_name} in {time}'''
        description = '\n'.join(tuple(value for value in [players, variables_info, run_date, comment, verifier, status, reason] if value))
        video_url = run.get('run_video')
        profile_picture = next(iter(player_obj.get_value('user_pfp')))
        return {'title': title, 'description': description, 'video_url': video_url, 'profile_picture': profile_picture}


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
