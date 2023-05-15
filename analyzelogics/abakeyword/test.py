
def removedup(x):
    mset =set(x.split(' '))
    words =' '.join(mset)
    return words

a='gaming homall desk'
print(removedup(a))