import random as rn
import pandas as pd

user_id = 1320
enabled_list = ['True', 'False']
search_type = ['Rental', 'Sales', '']
date = 20210701
user_data = []
user_id_list = []
rn.seed(100)

while date < 20210730:
    user_data = []
    user_id_list = []
    for rec in range(1, 10000):
        ud = ''
        if rec % 10 == 0:
            x = rn.sample([1, 10], 1)[0]
        else:
            x = 1
        for rec2 in range(x):
            ud += r'--\\n-:search_id:' + str(rn.randrange(0, 6000000)) + r'\\n:enabled:' + str(
                rn.sample(enabled_list, 1)[0]) + \
                      r'\\n:clicks:' + str(rn.randrange(1, 10)) + \
                      r'\\n:created_at:' + str(date) + r'\\n:listings:' + \
                      str(rn.randrange(1, 10)) + \
                      r'\\n:type:' + str(rn.sample(search_type, 1)[0]) + r'\n'
        user_data.append(ud)
        user_id_list.append(user_id)
        user_id += 1
    user_dict = {'user_id': user_id_list, 'user_data': user_data}
    df = pd.DataFrame(user_dict)
    file_name = 'inferred_users' + str(date)
    compression_opts = dict(method='zip',
                            archive_name=file_name + '.csv')
    df.to_csv(file_name + '.zip', compression=compression_opts, index=False)
    date += 1

print('------done--------')
