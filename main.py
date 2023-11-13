import discord
import datetime
import sqlite3
from json import dumps, loads
from discord import app_commands
import config
import asyncio
from traceback import print_exc
import tables
import players
import speedruncom_integration as src
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
    return runs_table.select_rows()


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


def get_num_verified(verifier_id=None, game_id=None):
    return len([run for run in runs_table.select_row_col('run_id', game_id=game_id, verifier=verifier_id, check_not_null=['''verify_date'''])])


def get_length_runs(game_id: str = ''):
    return sum(time.get('igt') for time in runs_table.select_row_col('igt', game_id=game_id))


@debounce
async def autocomplete_get_game(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    return [app_commands.Choice(name=game.get('names').get('international'), value=game.get('id'))
            for game in src.get_game(name=current).get('data')][:25]


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




