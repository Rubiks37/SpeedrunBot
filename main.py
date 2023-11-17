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
import users
import speedruncom_integration as src
from where import WhereCond
from debounce import Debounce


# set up all the adapters and converters for the different types of vars used in tables
sqlite3.register_adapter(datetime.date, tables.adapt_date_iso)
sqlite3.register_adapter(users.Users, tables.adapt_users)
sqlite3.register_adapter(dict, dumps)
sqlite3.register_converter('date', tables.convert_date_iso)
sqlite3.register_converter('users', tables.convert_users)
sqlite3.register_converter('json', loads)
conn = sqlite3.connect('runs.db', detect_types=sqlite3.PARSE_DECLTYPES)
conn.row_factory = tables.dict_factory

# creates variables table to handle all variables
variables_table = tables.VariableTable(conn)

# creates categories to handle all categories
categories_table = tables.CategoryTable(conn)

# creates user table to handle all users
user_table = tables.UserTable(conn)

# creates master table to combine all tables into one
master_table = tables.MasterTable(conn)

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


def resync_variables():
    all_variable_rows = []
    for game_id in config.GAMES:
        variables = src.parse_variables_into_rows(src.get_all_variables(game_id))
        all_variable_rows.extend(variables)
    variables_table.resync_table(all_variable_rows)


def resync_users():
    all_runs_rows = []
    for game_id in config.GAMES:
        runs = src.get_all_runs_users(game_id)
        all_runs_rows.extend(runs)
    users_rows = src.parse_runs_into_users_rows(all_runs_rows.copy())
    user_table.resync_table(src.duplicate_remover(users_rows, 0))
    return True


def resync_categories():
    all_category_rows = []
    for game_id in config.GAMES:
        categories = src.get_all_categories(game_id)
        all_category_rows.extend(categories)
    all_category_rows = src.parse_categories_into_rows(all_category_rows.copy())
    categories_table.resync_table(all_category_rows)
    return True


def resync_master_user():
    all_runs = []
    [all_runs.extend(src.get_all_runs_users(game_id)) for game_id in config.GAMES]
    user_rows = src.parse_runs_into_users_rows(all_runs)
    user_table.resync_table(user_rows)
    master_rows = [src.parse_call_into_master_row(run, categories_table, variables_table, user_table) for run in all_runs]
    master_table.resync_table(master_rows)


def resync_all():
    resync_categories()
    resync_variables()
    resync_master_user()


def get_master_table():
    return master_table(conn)


def get_wr():
    pass


def get_num_verified(*args):
    pass


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




