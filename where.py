import datetime
from re import findall
# a problem came up with select row col that im going to solve with a custom where condition class
# this class represents an argument to where. it has 3 variables


class WhereCond:
    def __init__(self, col, operator, value, col_mod: str = None):
        self.col = col
        self.operator = operator
        self.value = value
        self.col_mod = col_mod

    def __call__(self):
        return f'{self.col_mod}({self.col}) {self.operator} ?' if self.col_mod else f'{self.col} {self.operator} ?'


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