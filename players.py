class Players:
    def __init__(self, player_ids: tuple):
        self.player_ids = player_ids

    def __repr__(self):
        return ', '.join(self.player_ids)

    def __eq__(self, compare):
        return set(self.player_ids) == set(compare.player_ids)


# players should be run.get('players') if run is called
def get_player_from_run_api(players):
    player_ids = []
    for player in players:
        if player.get('rel') == 'user':
            player_ids.append(player.get('id'))
        elif player.get('rel') == 'guest':
            player_ids.append('guest_' + player.get('name'))
    return Players(tuple(player_ids))


def get_players_from_repr(players_str: str):
    return Players(tuple(players_str.split(", ")))
