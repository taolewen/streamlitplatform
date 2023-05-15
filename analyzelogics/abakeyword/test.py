def removedup(x):
    x = x.replace('+', ' ')
    mset = set(x.split(' '))
    words = ' '.join(mset)
    return words


a='gaming homall desk+desk iiii'
print(removedup(a))