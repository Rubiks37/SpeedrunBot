# ALL SRC functions that are built to interact with the SRC API will go here
# also any functions that help go through src responses will go here

from datetime import date
import requests
import config
import users
import tables
from where import WhereCond

header = config.HEADER
url = 'https://www.speedrun.com/api/v1/'


# returns a game response
def get_game(name: str = '', game_id: str = '') -> dict:
    if game_id:
        response = requests.get(url + 'games/' + game_id)
    else:
        params = {'name': name}
        response = requests.get(url + 'games', params=params, headers=header)
    return response.json()


def get_all_variables(game_id: str):
    variables_url = url + f'games/{game_id}/variables'
    params = {
        'max': 200
    }
    return iterate_through_responses(variables_url, params)


def get_all_categories(game_id: str):
    categories_url = url + f'games/{game_id}/categories'
    params = {
        'max': 200
    }
    categories = iterate_through_responses(categories_url, params)
    # this seems like the best way to get the game_id attached to the category
    [category.update({'game_id': game_id}) for category in categories]
    return categories


def get_all_runs_users(game_id: str):
    runs_url = url + 'runs'
    params = {
        'game': game_id,
        'max': 200,
        'embed': 'players',
        'orderby': 'date',
        'direction': 'desc'
    }
    runs = iterate_through_responses(runs_url, params)
    unique_runs = duplicate_remover(runs, lambda x: x.get('id'))
    return unique_runs


# since the api has a max request, we need to iterate through them sometimes, so this function does that
def iterate_through_responses(p_url: str, params: dict, limit: int = -1):
    all_responses = []
    while True:
        response = requests.get(p_url, params=params, headers=header)
        if response.status_code is not requests.codes.ok:
            raise requests.HTTPError(f'error: code {response.status_code}')
        data = response.json()
        all_responses.extend(data.get('data'))
        # we check if the size of the request is less than the size we requested for to see if we've hit the end
        # (indicating the end of the sequence we were requesting)
        pagination = data.get('pagination')
        if pagination:
            if pagination.get('size') < params.get('max') or (pagination.get('offset') > limit != -1):
                break
            uris = data.get('pagination').get('links')
            for uri in uris:
                if uri.get('rel') == 'next':
                    p_url = uri.get('uri')
                    break
        else:
            break
    return all_responses


def duplicate_remover(entries, primary_key_func):
    unique = set()
    unique_entries = []
    for entry in entries:
        primary_key = primary_key_func(entry)
        if primary_key not in unique:
            unique.add(primary_key)
            unique_entries.append(entry)
    return unique_entries


# mostly a debugging method to make sure im doing things right
def get_unverified(game_id: str = ''):
    params = {'status': 'new', 'game': game_id}
    response = requests.get(url + 'runs', params=params, headers=header).json()
    if response.status_code is not requests.codes.ok:
        raise requests.HTTPError(f'error: code {response.status_code}')
    return response


# takes the list of players and converts them into the format that my db is storing them in
# MUST EMBED PLAYERS FOR THIS TO WORK
def parse_runs_into_rows(runs):
    rows = []
    for run in runs:
        run_id = run.get('id')
        game_id = run.get('game')
        players_obj = users.get_user_from_run_api(run.get('players').get('data'))
        date_run = date.fromisoformat(run.get('date'))
        times = run.get('times')
        rta = times.get('realtime_t')
        igt = times.get('ingame_t') if times.get('ingame_t') != 0 else rta
        category = run.get('category')
        variables = run.get('values')
        status = run.get('status')
        verifier = status.get('examiner')
        verify_date = date.fromisoformat(status.get('verify-date')[:10]) if status.get('verify-date') else None
        rows.append((run_id, game_id, players_obj, date_run, rta, igt, category, variables, verifier, verify_date))
    return rows


def parse_variables_into_rows(variables):
    rows = []
    for variable in variables:
        variable_id = variable.get('id')
        name = variable.get('name')
        values = {key: value.get('label') for key, value in variable.get('values').get('values').items()}
        rows.append((variable_id, name, values))
    return rows


def parse_categories_into_rows(categories):
    category_rows = []
    for category in categories:
        category_id = category.get('id')
        category_name = category.get('name')
        category_game_id = category.get('game_id')
        category_rows.append((category_id, category_name, category_game_id))
    return category_rows


def parse_runs_into_users_rows(runs):
    user_rows = list()
    for run in runs:
        data = run.get('players').get('data')
        for player in data:
            player_type = player.get('rel')
            if player_type == 'user':
                name = player.get('names').get('international')
                user_id = player.get('id')
            else:
                name = player.get('name')
                user_id = 'guest_' + player.get('name')
            pronouns = player.get('pronouns')
            profile_pic = player.get('assets', {}).get('image', {}).get('uri')
            user_row = (user_id, name, pronouns, player_type, profile_pic)
            user_rows.append(user_row)
    user_rows = duplicate_remover(user_rows.copy(), lambda x: x[0])
    return user_rows


def parse_call_into_master_row(run: dict,
                               category_table: tables.CategoryTable,
                               variable_table: tables.VariableTable,
                               user_table: tables.UserTable):
    run_id = run.get('id')
    game_id = run.get('game')
    game_name = config.GAMES.get(game_id)
    player_data = run.get('players').get('data')
    users_obj = users.get_user_from_user_embed_api(player_data)
    users_name = ', '.join(users_obj.get_value('user_name'))
    date_run = date.fromisoformat(run.get('date'))
    times = run.get('times')
    rta = times.get('realtime_t')
    igt = times.get('ingame_t') if times.get('ingame_t') != 0 else rta
    category = run.get('category')
    category_row = next(iter(category_table.select_row_col(cols=['name'], where_conds=[WhereCond('category_id', '=', category)])), None)
    category_name = category_row.get('name') if category_row else None
    variables = run.get('values')
    variable_rows = [variable_table.select_row_col(
            cols=[f'''var_name, JSON_EXTRACT(var_values, '$.{variables[variable]}') AS 'var_values' '''],
            where_conds=[WhereCond('variable_id', '=', variable)])[0] for variable in variables]
    variables_info = {variable.get('var_name'): variable.get('var_values') for variable in variable_rows}
    status_dict = run.get('status')
    verifier = status_dict.get('examiner')
    verifier_row = next(iter(user_table.select_row_col(where_conds=[WhereCond('user_id', '=', verifier)])), None)
    verifier_info = users.get_user_from_user_row(verifier_row) if verifier_row else None
    verifier_name = verifier_info.get_value('user_name')[0] if verifier_row else None
    verify_date = date.fromisoformat(status_dict.get('verify-date')[:10]) if status_dict.get('verify-date') else None
    status = status_dict.get('status')
    return (
        run_id,
        game_id,
        game_name,
        date_run,
        users_obj,
        users_name,
        rta,
        igt,
        category,
        category_name,
        variables,
        variables_info,
        verifier_info,
        verifier_name,
        verify_date,
        status
    )
