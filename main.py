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


def get_master_table():
    return master_table(conn)


def get_wr():
    pass


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
        selected_cols = ['game_name', 'player_info', 'run_date', 'run_video', 'comment', 'rta', 'igt', 'category_name', 'variable_info', 'verifier_info', 'status', 'reason']
        run = next(iter(master_table.select_row_col(cols=selected_cols, where_conds=[WhereCond('run_id', '=', run)])), [])

        game_name = run.get('game_name')
        category_name = run.get('category_name')
        time = ac.format_time(run.get('igt'))
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
        description = '\n'.join(tuple(value for value in [players, variables_info, run_date, comment, verifier, status, reason] if value))

        video_url = run.get('run_video')
        profile_picture = next(iter(player_obj.get_value('user_pfp')))

        embed = discord.Embed(title=f'''{game_name} {category_name} in {time}''',
                              description=description,
                              url=video_url)
        embed.set_image(url=profile_picture)
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
