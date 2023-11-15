import discord
import datetime
import sqlite3
from json import dumps, loads
from discord import app_commands
import config
import asyncio
from traceback import print_exc
from re import findall
import tables
import players
import speedruncom_integration as src
from where import WhereCond
from debounce import Debounce


# set up all the adapters and converters for the different types of vars used in tables
sqlite3.register_adapter(datetime.date, tables.adapt_date_iso)
sqlite3.register_adapter(players.Players, tables.adapt_players)
sqlite3.register_adapter(dict, dumps)
sqlite3.register_converter('date', tables.convert_date_iso)
sqlite3.register_converter('players', tables.convert_players)
sqlite3.register_converter('json', loads)
conn = sqlite3.connect('runs.db', detect_types=sqlite3.PARSE_DECLTYPES)
conn.row_factory = tables.dict_factory

# create RunTable to handle all cached runs
runs_table = tables.RunTable(conn)

# creates variables table to handle all variables
variables_table = tables.VariableTable(conn)

# create debounce object
debounce = Debounce(1)

intents = discord.Intents.default()
intents.message_content = True
allowed_mentions = discord.AllowedMentions.none()
client = discord.Client(intents=intents, allowed_mentions=allowed_mentions)
tree = app_commands.CommandTree(client)


async def sync_commands():
    await tree.sync()


def split_message(content):
    if len(content) <= 2000:
        return [content]
    fragments = []
    while len(content) > 2000:
        cutoff = content[:2000].rfind('\n')
        fragments.append(content[:cutoff])
        content = content[cutoff:].lstrip()
    fragments.append(content)
    return fragments


def resync_runs():
    all_run_rows = []
    for game_id in config.GAMES:
        runs = src.parse_runs_into_rows(src.get_all_runs(game_id))
        all_run_rows.extend(runs)
    runs_table.resync_table(all_run_rows)


def get_all_runs():
    return runs_table.select_row_col()


def resync_variables():
    all_variable_rows = []
    for game_id in config.GAMES:
        variables = src.parse_variables_into_rows(src.get_all_variables(game_id))
        all_variable_rows.extend(variables)
    variables_table.resync_table(all_variable_rows)


def get_master_table():
    return tables.master_table(conn)


def get_wr():
    pass


def get_num_verified(*args):
    pass


def get_length_runs(game_id: str = ''):
    return sum(time.get('igt') for time in runs_table.select_row_col(cols=('igt',)))


def get_dates_from_str(string: str):
    # regex of iso8401 dates is /d/d/d/d-/d/d-/d/d
    return findall('/d/d/d/d-/d/d-/d/d', string)


def create_where_conditions_from_date_str(date_str: str) -> tuple:
    keyword = date_str.lower().split(' ')[0]
    normal_keyword_values = {
        'after': 'date > {}',
        'before': 'date < {}',
        'on': 'date = {}',
        'between': '{} > date;{} < date',
    }
    if keyword in normal_keyword_values:
        dates = get_dates_from_str(date_str)
        if not dates:
            raise ValueError('error: date not in correct format')
        where_cond_tuple = tuple(map(lambda x: x.split(' '), normal_keyword_values[keyword].format(dates).split(';')))
        where_conds = tuple(WhereCond(cond[0], cond[1], cond[2]) for cond in where_cond_tuple)
        return where_conds
    elif keyword == 'last':
        date = datetime.date.fromisoformat(get_dates_from_str(date_str)[0])
        num = int(date_str.split(' ')[1])
        if 'days' in date_str:
            return (WhereCond('date', '>', date - datetime.timedelta(days=num)),)
        elif 'weeks' in date_str:
            return (WhereCond('date', '>', date - datetime.timedelta(weeks=num)),)
        elif 'months' in date_str:
            return (WhereCond('date', '>', date - datetime.timedelta(days=num*30)),)
        elif 'years' in date_str:
            return (WhereCond('date', '>', date - datetime.timedelta(days=num*365)),)
        else:
            raise ValueError('error: date not in correct format')
    elif keyword == 'year':
        year = findall('/d/d/d/d', date_str)
        if not year:
            raise ValueError('error: date not in correct format')
        conditions = (f'date >= 01-01-{year}', f'date <= 12-31-{year}')
        return tuple(WhereCond(cond[0], cond[1], cond[2]) for cond in conditions)
    else:
        raise ValueError('error: date not in correct format')


@debounce
async def autocomplete_get_game(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    return [app_commands.Choice(name=game.get('names').get('international'), value=game.get('id'))
            for game in src.get_game(name=current).get('data')][:25]


async def autocomplete_get_date(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    keywords = {
        'after': ['After YYYY-MM-DD'],
        'before': ['Before YYYY-MM-DD'],
        'on': ['On YYYY-MM-DD'],
        'between': ['Between YYYY-MM-DD and YYYY-MM-DD'],
        'last': ['Last # Days', 'Last # Weeks', 'Last # Months', 'Last # Years'],
        'year': ['Year YYYY']
    }
    starts_with = current.lower().split(' ')[0]
    if starts_with in keywords:
        values = keywords[starts_with]
        return [app_commands.Choice(name=val, value=val) for val in values]
    else:
        return [app_commands.Choice(name=val, value=val) for key, value in keywords.items() for val in value if current.lower().split(' ')[0] in key]


@client.event
async def on_ready():
    await sync_commands()


@tree.command(name='get_game', description='find a game\'s id')
@app_commands.autocomplete(name=autocomplete_get_game)
async def cmd_get_game(interaction: discord.Interaction, name: str):
    try:
        await interaction.response.send_message(content=f'the id is {name}')
    except Exception as error:
        print_exc()
        await interaction.response.send_message(content=error)


@tree.command(name='get_number_of_verified', description='get the total number of verified runs for the games monitored')
@app_commands.autocomplete(date=autocomplete_get_date)
async def cmd_get_num_verified(interaction: discord.Interaction, date: str = None):
    try:
        await interaction.response.send_message(content=f'num verified is none because i havent implemented this')
    except Exception as error:
        print_exc()
        await interaction.response.send_message(content=error)


@tree.command(name='sync', description='MOD ONLY: syncs the application commands')
async def sync(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        await sync_commands()
        await interaction.followup.send(content="sync successful")
    except Exception as error:
        await interaction.followup.send(content=error)

if __name__ == '__main__':
    client.run(config.TOKEN)
    conn.commit()
    # games = (src.get_game(name='subnautica category extensions').get('data'))




