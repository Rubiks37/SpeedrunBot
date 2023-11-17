from json import dumps, loads


class Users:
    def __init__(self, users_dict: dict = None):
        self.users = users_dict if users_dict else dict()

    def __repr__(self):
        return dumps(self.users)

    def __eq__(self, compare):
        return set(self.users) == set(compare.users)

    def add_user(self, users_id: str, users_type: str, users_name: str = None, pronouns: str = None, users_pfp: str = None):
        self.users.update({users_id: {
            'user_name': users_name,
            'user_type': users_type,
            'pronouns': pronouns,
            'user_pfp': users_pfp
        }})

    def get_value(self, value):
        return [user.get(value) for user in self.users.values()]


# users should be run.get('users') if run is called
def get_user_from_run_api(users):
    users_obj = Users()
    for user in users:
        user_type = user.get('rel')
        if user.get('rel') == 'user':
            user_id = user.get('id')
        else:
            user_id = 'guest_' + user.get('name')
        users_obj.add_user(user_id, user_type)
    return users_obj


def get_user_from_user_row(user_row: dict):
    user_id = user_row.pop('user_id')
    return Users({user_id: user_row})


def get_user_from_user_embed_api(users):
    users_obj = Users()
    for user in users:
        user_type = user.get('rel')
        if user_type == 'user':
            name = user.get('names').get('international')
            user_id = user.get('id')
        else:
            name = user.get('name')
            user_id = 'guest_' + user.get('name')
        pronouns = user.get('pronouns')
        profile_pic = user.get('assets', {}).get('image', {}).get('uri')
        users_obj.add_user(user_id, user_type, name, pronouns, profile_pic)
    return users_obj


def get_users_from_repr(users_str):
    return Users(loads(users_str))
