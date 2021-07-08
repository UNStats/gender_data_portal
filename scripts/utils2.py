def col_names_to_uppercase(x):
    '''
    convert all columns in a dataset to uppercase
    input: x is a list of dictionaries
    '''
    new_x = []
    for i in x:
        newdict = dict()
        for k in i.keys():
            newdict[k.upper()] = i[k]
        new_x.append(newdict)
    return new_x