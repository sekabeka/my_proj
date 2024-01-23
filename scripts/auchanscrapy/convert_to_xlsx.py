import pandas as pd
import json


def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v


def filter(object:pd.DataFrame, key:str):
    return object.drop_duplicates(subset=[key])

bad_brands = [
    i.upper() for i in  pd.read_excel('AUCHAN/brands.xlsx').to_dict('list')[0]
]
with open('auchan_test.jsonl', encoding='utf-8', mode='r') as file:
    s = file.readlines()
result = [json.loads(i) for i in s]
for item in result:
    if item['Параметр: Бренд'].upper() in bad_brands:
        del item
    else:
        continue
with pd.ExcelWriter('auchan_result.xlsx', mode='w', engine_kwargs={'options': {'strings_to_urls': False}}, engine='xlsxwriter') as writer:
    for name, res in df(result, 'name'):
        p = pd.DataFrame(res)
        p.to_excel(writer, sheet_name=name.upper(), index=False)
    keys = [
        'Название товара или услуги',
        'Артикул',
        'Старая цена',
        'Остаток',
        'Цена закупки',
        'Цена продажи',
        'Параметр: Group',
        'Параметр: Auch'
    ]
    key = 'Параметр: Артикул поставщика'
    p = pd.DataFrame(result)
    p = filter(p,key)
    tmp = p.to_dict('list')
    for key in list(tmp.keys()):
        if key in keys:
            pass
        else:
            tmp.pop(key)
    p.to_excel(writer, index=False, sheet_name='result')
    pd.DataFrame(tmp).to_excel(writer, index=False, sheet_name='result_1')