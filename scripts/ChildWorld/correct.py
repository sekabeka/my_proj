
# roots = set([i['number'] for i in result])


def df(lst, key):
    result = {i : [] for i in set([i["number"] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for dictionary in result:
        yield dictionary



import json, pandas as pd
with open('results/child.jsonl', 'r', encoding='utf-8') as file:
    s = file.readlines()
result = [json.loads(item) for item in s]

p = pd.DataFrame(result)
with pd.ExcelWriter('results/child.xlsx', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls': False}}) as writer:
    p.to_excel(writer, index=False, sheet_name='products')
    main_headers = [
        'Название товара или услуги',
        'Цена закупки',
        'Старая цена',
        'Артикул',
        'Параметр: Размер скидки',
        'Параметр: Остаток',
        'Цена продажи',
        'Параметр: Deti',
        'Параметр: Group'
    ]
    r = []
    for item in result:
        tmp = {}
        for key in item.keys():
            if key in main_headers:
                tmp[key] = item[key]
        r.append(tmp)
    p = pd.DataFrame(r)
    p.to_excel(writer, sheet_name='short_prod', index=False)
    for d in df(result, "number"):








