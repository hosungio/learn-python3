# https://stackoverflow.com/questions/1894269/how-to-convert-string-representation-of-list-to-a-list

import ast
import json

x = '[ "A","B","C" , " D"]'
x_list = ast.literal_eval(x)
print(x_list, type(x_list))

x_list = json.loads(x)
print(x_list, type(x_list))

x = '["foo",44,null]'
try:
    x_list = ast.literal_eval(x)
    print(x_list, type(x_list))
except Exception as ex:
    print(ex)

x = '["foo",44,null]'
try:
    x_list = json.loads(x)
    print(x_list, type(x_list))
except Exception as ex:
    print(ex)


x = '["foo",44,"2020-01-02",176.8,67.8]'
x_list = ast.literal_eval(x)
print(x_list, [type(e) for e in x_list])
