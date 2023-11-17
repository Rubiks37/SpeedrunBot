import discord
from discord.app_commands import Choice
from debounce import Debounce
import speedruncom_integration as src
import tables

# create debounce object
debounce = Debounce(1)


# autocompletes fail if the name/value is over 100 characters, so this accounts for that
def slice_names_100(name):
    length = len(name)
    if length <= 100:
        return name
    r_index = 85
    cut_title = name[:r_index + 1] + "..." + name[-1 * (97 - r_index) + 1:]
    return cut_title


def search_list(current_str: str, strs_to_search: tuple):
    translate_table = str.maketrans('', '', '\'",-.!/():')
    current_words = current_str.translate(translate_table).split(' ')
    return_list = list()
    for item in strs_to_search:
        tuple_to_search = tuple(item[0].translate(translate_table).split(' '))
        if all([any([string1.lower() in string2.lower() for string2 in tuple_to_search]) for string1 in current_words]):
            return_list.append(item)
    return return_list


def format_time(total_seconds):
    hours, remainder = divmod(total_seconds, 3600)
    minutes, remainder = divmod(remainder, 60)
    seconds, milliseconds = divmod(remainder, 1)
    format_params = (seconds,)
    format_str = '{}'
    if minutes > 0:
        format_str = '{}:{:02}'
        format_params = (minutes, seconds)
    if hours > 0:
        format_str = '{}:{:02}:{:02}'
        format_params = (hours, minutes, seconds)
    if milliseconds > 0:
        format_str += '.{:03}'
        format_params += tuple([round(milliseconds*1000, 0)])
    return format_str.format(*map(int, format_params))


# less specific usage to get date autocomplete for many commands
async def get_date(interaction: discord.Interaction, current: str) -> list[Choice[str]]:
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
        return [Choice(name=val, value=val) for val in values]
    else:
        return [Choice(name=val, value=val) for key, value in keywords.items() for val in value if current.lower().split(' ')[0] in key]


@debounce
async def get_game(interaction: discord.Interaction, current: str) -> list[Choice[str]]:
    return [Choice(name=game.get('names').get('international'), value=game.get('id'))
            for game in src.get_game(name=current).get('data')][:25]


# since this function requires access to the master table, we wrap the function and return the autocomplete function
def get_run(master_table: tables.MasterTable):
    async def get_run_inner(interaction: discord.Interaction, current: str) -> list[Choice[str]]:
        selected_cols = ['run_id', 'game_name', 'player_name', 'igt', 'category_name', 'variable_info']
        rows = tuple((f'''{row.get('game_name')}: {row.get('category_name')} by {row.get('player_name')} in {format_time(row.get('igt'))} ({' '.join(row.get('variable_info').values())})''', row.get('run_id'))
                     for row in master_table.select_row_col(cols=selected_cols))
        filtered_rows = search_list(current, rows)
        return [Choice(name=row[0], value=row[1]) for row in filtered_rows][:25]

    return get_run_inner
