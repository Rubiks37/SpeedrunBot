import discord
import datetime
import sqlite3
from json import dumps, loads
from discord import app_commands
import config
import asyncio
from traceback import print_exc
import tables
import users
import speedruncom_integration as src
from where import WhereCond
import autocomplete as ac


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

intents = discord.Intents.default()
intents.message_content = True
allowed_mentions = discord.AllowedMentions.none()
client = discord.Client(intents=intents, allowed_mentions=allowed_mentions)
tree = app_commands.CommandTree(client)


async def sync_commands():
    await tree.sync()
    await tree.sync(guild=await client.fetch_guild(843315943836614676))


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
    tables.drop_all_tables(conn)
    resync_categories()
    resync_variables()
    resync_master_user()


def get_wr(autocomplete_val: str):
    name, category, variables = loads(autocomplete_val)
    name = WhereCond('game_name', '=', name)
    category = WhereCond('category_name', '=', category)
    status = WhereCond('status', '=', 'verified')
    variables = [WhereCond('variable_info', 'LIKE', f"%{variable}%") for variable in variables.copy()]
    row = master_table.select_row_col(cols=['run_id'], where_conds=[name, category, *variables, status], append='ORDER BY igt LIMIT 1')
    run_id = next(iter(row), {}).get('run_id')
    return master_table.get_embed_attributes_from_run_id(run_id, ac.format_time)


def get_num_verified(*args):
    pass


def get_run(run_id: str):
    run_id_where = WhereCond('run_id', '=', run_id)
    return next(iter(master_table.select_row_col(where_conds=[run_id_where])))


@client.event
async def on_ready():
    pass


@tree.command(name='get_game', description='find a game\'s id')
@app_commands.autocomplete(name=ac.get_game)
async def cmd_get_game(interaction: discord.Interaction, name: str):
    try:
        await interaction.response.send_message(content=f'the id is {name}')
    except Exception as error:
        print_exc()
        await interaction.response.send_message(content=error)


@tree.command(name='get_number_of_verified', description='get the total number of verified runs for the games monitored')
@app_commands.autocomplete(date=ac.get_date)
async def cmd_get_num_verified(interaction: discord.Interaction, date: str = None):
    try:
        await interaction.response.send_message(content=f'num verified is none because i havent implemented this')
    except Exception as error:
        print_exc()
        await interaction.response.send_message(content=error)


@tree.command(name='get_run', description='gets a specific run')
@app_commands.autocomplete(run=ac.get_run(master_table))
async def cmd_get_run(interaction: discord.Interaction, run: str):
    try:
        embed_attributes = (master_table.get_embed_attributes_from_run_id(run, ac.format_time))
        embed = discord.Embed(title=embed_attributes.get('title'),
                              description=embed_attributes.get('description'),
                              url=embed_attributes.get('video_url'))

        embed.set_image(url=embed_attributes.get('profile_picture'))
        await interaction.response.send_message(embed=embed)
    except Exception as error:
        print_exc()
        await interaction.response.send_message(content=error)


@tree.command(name='get_wr', description='gets a world record for a specified category')
@app_commands.autocomplete(run_category=ac.get_categories(variables_table))
async def cmd_get_wr(interaction: discord.Interaction, run_category: str):
    try:
        embed_attributes = get_wr(run_category)
        embed = discord.Embed(title=embed_attributes.get('title'),
                              description=embed_attributes.get('description'),
                              url=embed_attributes.get('video_url'))

        embed.set_image(url=embed_attributes.get('profile_picture'))
        await interaction.response.send_message(embed=embed)
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
