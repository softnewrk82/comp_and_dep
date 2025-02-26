from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


#__________________________________


import requests
import json 

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

import warnings
warnings.simplefilter("ignore")


import modules.api_info

import importlib
importlib.reload(modules.api_info) 


import datetime

import pendulum



from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encrypt_var_db_name_for_upl
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")

var_db_name_for_upl = f_decrypt(var_encrypt_var_db_name_for_upl, load_key_external()).decode("utf-8")


var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")

API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")






date_now = datetime.datetime.now().date()

default_arguments = {
    'owner': 'evgenijgrinev',
    'retries': 1,
    'retry_delay': timedelta(seconds=300),
    'email': ['grinev96@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,

}

with DAG(
    'sbis_comp_and_dep',
    schedule_interval='0 0 1-4 1-12 *',
    catchup=False,
    default_args=default_arguments,
    start_date=days_ago(1),

) as dag:

    def processing_sbis_comp_and_dep():

        from modules.api_info import var_app_client_id, var_app_secret, var_secret_key, var_db_user_name, var_db_user_pass, var_db_host, var_db_port, var_db_name, var_db_schema, var_db_name_for_upl
        from modules.api_info import var_db_user_name, var_db_user_pass, var_db_host, var_db_port, var_db_name
        from modules.api_info import var_db_schema, var_db_name_for_upl


        # ____________________________________________________________________________________________
        var_app_client_id = var_app_client_id
        var_app_secret = var_app_secret
        var_secret_key = var_secret_key

        var_db_user_name = var_db_user_name
        var_db_user_pass = var_db_user_pass
        var_db_host = var_db_host
        var_db_port = var_db_port

        var_db_name = var_db_name

        var_db_schema = var_db_schema
        var_db_name_for_upl = var_db_name_for_upl
        # ____________________________________________________________________________________________


        json_sbis={"app_client_id":var_app_client_id,"app_secret":var_app_secret,"secret_key":var_secret_key}
        url = 'https://online.sbis.ru/oauth/service/'
        response = requests.post(url, json=json_sbis)
        response.encoding = 'utf-8'


        str_to_dict = json.loads(response.text)


        access_token = str_to_dict["access_token"]


        parameters = {
            'withPhones': 'true',
            'withPrices': 'true'
        }
        url = 'https://api.sbis.ru/retail/point/list?'
        headers = {
        "X-SBISAccessToken": access_token
        }
        response_points = requests.get(url, params=parameters, headers=headers)

        str_to_dict_points = json.loads(response_points.text)


        # var_id = str_to_dict_points["salesPoints"][0]["id"]
        # var_sales = str_to_dict_points["salesPoints"][0]["prices"][0]


        for i in range(len(str_to_dict_points["salesPoints"])):
            try:
                var_id = str_to_dict_points["salesPoints"][i]["id"]
                var_sales = str_to_dict_points["salesPoints"][i]["prices"][0]
                print(var_id, var_sales)
            except:
                pass

        # ____________________________________________________________________________________________
        # ____________________________________________________________________________________________
        # ____________________________________________________________________________________________


        var_status_has_more_comp_list = "Да"
        j_page = 0

        doc_trade = []
        comp_id = []
        comp_infsys_id = []
        restrict_sign_comp = []
        comp_inn = []
        comp_kpp = []
        comp_full_name = []

        while var_status_has_more_comp_list == "Да":

            parameters_real = {
            "jsonrpc": "2.0",
            "method": "СБИС.СписокНашихОрганизаций",
            "params": {
                "Фильтр": {
                        "Навигация": {
                        "РазмерСтраницы": "20",
                        "Страница": j_page
                        }
                },
            },
            "id": 0
            }

            url_real = "https://online.sbis.ru/service/?srv=1"

            response_points = requests.post(url_real, json=parameters_real, headers=headers)
            str_to_dict_points_main = json.loads(response_points.text)

            json_data_points = json.dumps(str_to_dict_points_main, ensure_ascii=False, indent=4).encode("utf8").decode()

            with open("DICT_Comp.json", 'w') as json_file_points_o:
                json_file_points_o.write(json_data_points)

            for l in str_to_dict_points_main["result"]["НашаОрганизация"]:



                doc_trade.append(l["ДокументооборотПодключен"])
                comp_id.append(l["Идентификатор"])
                comp_infsys_id.append(l["ИдентификаторИС"])
                restrict_sign_comp.append(l["ПодписаниеОграничено"])

                try:
                    comp_inn.append(l["СвЮЛ"]["ИНН"])
                    comp_kpp.append(l["СвЮЛ"]["КПП"])
                    if l["СвЮЛ"]["НазваниеПолное"] != "":
                        comp_full_name.append(l["СвЮЛ"]["НазваниеПолное"])
                    elif l["СвЮЛ"]["НазваниеПолное"] != "":
                        comp_full_name.append(l["СвЮЛ"]["Название"])

                except:
                    comp_inn.append(l["СвФЛ"]["ИНН"])
                    comp_kpp.append(np.nan)

                    if l["СвФЛ"]["НазваниеПолное"] != "":
                        comp_full_name.append(l["СвФЛ"]["НазваниеПолное"])
                    elif l["СвФЛ"]["НазваниеПолное"] != "":
                        comp_full_name.append(l["СвФЛ"]["Название"])





            if str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Нет":
                var_status_has_more_comp_list = False

            elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
                j_page += 1


        col = [
            "doc_trade",
            "comp_id",
            "comp_infsys_id",
            "restrict_sign_comp",
            "comp_inn",
            "comp_kpp",
            "comp_full_name",
        ]


        df_comp = pd.DataFrame(columns= col, data=list(zip(
                doc_trade,
                comp_id,
                comp_infsys_id,
                restrict_sign_comp,
                comp_inn,
                comp_kpp,
                comp_full_name,
            ))
        )

        # ____________________________________________________________________________________________

        my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
        try:
            my_conn.connect()
            print(f'connected to {var_db_name}')

            my_conn = my_conn.connect()

            df_comp.to_sql(name=f'comp_sbis_source', con=my_conn, if_exists="replace")

        except:
            print(f'failed connection to {var_db_name}')

        print(f"Upload_part_to_{var_db_name} = completed!")
        print("____________")

        df_comp_red = df_comp[[
            "comp_inn",
            "comp_kpp",
            "comp_full_name",
        ]]


        # ____________________________________________________________________________________________

        comp_list_inn_comp = df_comp_red["comp_inn"].to_list()
        comp_list_kpp_comp = df_comp_red["comp_kpp"].to_list()
        comp_list_full_name_comp = df_comp_red["comp_full_name"].to_list()


        external_id = []
        inside_id = []
        code = []
        name = []
        chapter_inside_id = []
        chapter_inside_code = []
        chapter_inside_name = []
        dep_comp_inn = []
        dep_comp_kpp = []
        dep_comp_full_name = []


        for h in range(len(comp_list_inn_comp)):

            if comp_list_inn_comp[h] == '':
                pass
            else:
                var_status_has_more = "Да"
                i_page = 0
    
                while var_status_has_more == "Да":
    
    
                    var_inn = comp_list_inn_comp[h]
                    var_kpp = comp_list_kpp_comp[h]
                    var_full_name = comp_list_full_name_comp[h]
    
                    if len(var_inn) <= 10:
                        parameters_real = {
                        "jsonrpc": "2.0",
                        "method": "СБИС.СписокПодразделений",
                        "params": {
                            "Параметр": {
                                "Фильтр": {
                                    "НашаОрганизация": {
                                    "СвЮЛ": {
                                        "ИНН": var_inn,
                                        "КПП": var_kpp
                                    }
                                    }
                                },
                                "Навигация": {
                                    "РазмерСтраницы": "20",
                                    "Страница": i_page
                                }
                            }
                        }
                        }
    
                    elif len(var_inn) > 10:
                        parameters_real = {
                        "jsonrpc": "2.0",
                        "method": "СБИС.СписокПодразделений",
                        "params": {
                            "Параметр": {
                                "Фильтр": {
                                    "НашаОрганизация": {
                                    "СвФЛ": {
                                        "ИНН": var_inn,
                                    }
                                    }
                                },
                                "Навигация": {
                                    "РазмерСтраницы": "20",
                                    "Страница": i_page
                                }
                            }
                        }
                        }
    
                    url_real = "https://online.sbis.ru/service/?srv=1"
    
                    response_points = requests.post(url_real, json=parameters_real, headers=headers)
                    str_to_dict_points_main = json.loads(response_points.text)
    
    
                    json_data_points = json.dumps(str_to_dict_points_main, ensure_ascii=False, indent=4).encode("utf8").decode()
    
                    with open("DICT_Dep.json", 'w') as json_file_points_o:
                        json_file_points_o.write(json_data_points)
    
    
                    for k in str_to_dict_points_main["result"]["Подразделение"]:
    
                        external_id.append(k["ВнешнийИдентификатор"])
                        inside_id.append(k["Идентификатор"])
                        code.append(k["Код"])
                        name.append(k["Название"])
                        chapter_inside_id.append(k["Раздел"]["Идентификатор"])
                        chapter_inside_code.append(k["Раздел"]["Код"])
                        chapter_inside_name.append(k["Раздел"]["Название"])
    
                        dep_comp_inn.append(var_inn)
                        dep_comp_kpp.append(var_kpp)
                        dep_comp_full_name.append(var_full_name)
    
                    if str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Нет":
                        var_status_has_more = False
    
                    elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
                        i_page += 1


        col = [
            "external_id",
            "inside_id",
            "code",
            "name",
            "chapter_inside_id",
            "chapter_inside_code",
            "chapter_inside_name",

            "comp_inn",
            "comp_kpp",
            "comp_full_name",
        ]


        df_dep = pd.DataFrame(columns= col, data=list(zip(
                external_id,
                inside_id,
                code,
                name,
                chapter_inside_id,
                chapter_inside_code,
                chapter_inside_name,

                dep_comp_inn,
                dep_comp_kpp,
                dep_comp_full_name,

            ))
        )

        # df_dep["inside_id"] = df_dep["inside_id"].astype(float)
        # df_dep["chapter_inside_id"] = df_dep["chapter_inside_id"].astype(float)

        # ____________________________________________________________________________________________

        my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
        try:
            my_conn.connect()
            print(f'connected to {var_db_name}')

            my_conn = my_conn.connect()

            df_dep.to_sql(name=f'dep_sbis_source', con=my_conn, if_exists="replace")

        except:
            print(f'failed connection to {var_db_name}')

        print(f"Upload_part_to_{var_db_name} = completed!")
        print("____________")

        # ____________________________________________________________________________________________

        df_dep_hierarch = df_dep[["inside_id", "chapter_inside_id"]]
        df_dep_hierarch_name = df_dep[["inside_id", "chapter_inside_id", "name"]]

        dict_df_dep = dict(list(zip(df_dep_hierarch_name["inside_id"], df_dep_hierarch_name["name"])))

        names_lst = [
            "chapter_inside_id",
            "three",
            "four",
            "five",
            "six",
            "seven",
            "eight",
            "nine",
            "ten",
        ]

        for v in range(len(names_lst)):

            try:
                df_dep_hierarch[names_lst[v+1]] = ""

                for i in range(len(df_dep_hierarch[names_lst[v]])):
                    if df_dep_hierarch[names_lst[v]].iloc[i] in df_dep_hierarch["inside_id"].to_list():
                        df_dep_hierarch[names_lst[v+1]].iloc[i] = df_dep_hierarch[names_lst[0]].to_list()[df_dep_hierarch["inside_id"].to_list().index(df_dep_hierarch[names_lst[v]].iloc[i])]
            except:
                pass


        df_dep_hierarch["Parents_links"] = ""

        for i in range(len(df_dep_hierarch["Parents_links"])):
            lst_links = []
            for z in range(len(names_lst)):
                if df_dep_hierarch[names_lst[z]].iloc[i] == "":
                    pass
                else:
                    lst_links.append(df_dep_hierarch[names_lst[z]].iloc[i])
                    df_dep_hierarch["Parents_links"].iloc[i] = lst_links

        df_dep_hierarch_upd = df_dep_hierarch.drop(columns=names_lst[1:], axis=1)

        df_dep_hierarch_upd["Parents_names_links"] = ""
        df_dep_hierarch_upd["ActivityManagent"] = ""

        for i in range(len(df_dep_hierarch_upd["Parents_links"])):
            list_names_links = []
            for j in range(len(df_dep_hierarch_upd["Parents_links"].iloc[i])):
                try:
                    var_a = dict_df_dep[df_dep_hierarch_upd["Parents_links"].iloc[i][j]]
                    list_names_links.append(var_a)
                except:
                    pass
            df_dep_hierarch_upd["Parents_names_links"].iloc[i] = list_names_links
            try:
                df_dep_hierarch_upd["ActivityManagent"].iloc[i] = list_names_links[-1]
            except:
                df_dep_hierarch_upd["ActivityManagent"].iloc[i] = ""

        df_dep_hierarch_upd
        df_dep_hierarch_upd_red = df_dep_hierarch_upd[[
            "inside_id",
            "Parents_links",
            "Parents_names_links",
            "ActivityManagent",
        ]]

        df_dep_merged = df_dep.merge(df_dep_hierarch_upd_red, left_on='inside_id', right_on='inside_id')


        # ____________________________________________________________________________________________

        print("____________")

        my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name_for_upl}")
        try:
            my_conn.connect()
            print(f'connected to {var_db_name_for_upl}')

            my_conn = my_conn.connect()

            df_dep_merged.to_sql(name=f'dep_sbis', con=my_conn, schema=var_db_schema, if_exists="replace")

        except:
            print('failed connection to db_softumn')

        print(f"Upload_part_to_{var_db_name_for_upl} = completed!")
        print("____________")
        # ____________________________________________________________________________________________


    def check():
        print("ok")

    op1 = PythonOperator(
        task_id='check_task',
        python_callable=check
    )

    op2 = PythonOperator(
        task_id='upload_sbis_comp_and_dep',
        python_callable=processing_sbis_comp_and_dep
    )

op1 >> op2
