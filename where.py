
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
