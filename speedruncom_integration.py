# ALL SRC functions that are built to interact with the SRC API will go here
# also any functions that help go through src responses will go here

from datetime import date
import requests
import config
import players

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


# returns a list containing every run for a given game id.
# this command should not be run very often since it is api intensive.
def get_all_runs(game_id: str):
    runs_url = url + 'runs'
    params = {
        'game': game_id,
        'max': 200,
        'orderby': 'date',
        'direction': 'desc'
    }
    return duplicate_remover(iterate_through_responses(runs_url, params), 'id')


def get_all_variables(game_id: str):
    variables_url = url + f'games/{game_id}/variables'
    params = {
        'max': 200
    }
    return iterate_through_responses(variables_url, params)


# since the api has a max request, we need to iterate through them sometimes, so this function does that
def iterate_through_responses(p_url: str, params: dict, limit: int = -1, data_per_request: int = 200):
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


# for some reason, src sometimes fucks up and gives duplicate entries on things that should be unique, like runs,
# so we check for that and remove them
def duplicate_remover(responses, unique):
    unique_responses = []
    unique_values = set()
    for response in responses:
        value = response.get(unique)
        if value not in unique_values:
            unique_responses.append(response)
            unique_values.add(value)
    return unique_responses


# mostly a debugging method to make sure im doing things right
def get_unverified(game_id: str = ''):
    params = {'status': 'new', 'game': game_id}
    response = requests.get(url + 'runs', params=params, headers=header).json()
    if response.status_code is not requests.codes.ok:
        raise requests.HTTPError(f'error: code {response.status_code}')
    return response


# takes the list of players and converts them into the format that my db is storing them in
def parse_runs_into_rows(runs):
    rows = []
    for run in runs:
        run_id = run.get('id')
        game_id = run.get('game')
        players_obj = players.get_player_from_run_api(run.get('players'))
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
