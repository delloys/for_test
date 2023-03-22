from app import app
from flask import render_template, request, session
from utils import get_db_connection
import pandas as pd
from datetime import datetime
from models.index_model import *

@app.route('/', methods=['get', 'post'])
def index():

    conn = get_db_connection()

    df_table = pd.DataFrame([
                       [1, 'В наличии', 'get_tabl3'],
                       [2, 'Отсутствуют', 'get_tabl5']],
                      columns=['id', 'name', 'text'])

    if request.values.get('id_bike'):
        id_bike=(request.values.get('id_bike'))
    else:
        id_bike=-1

    count_plus = 0
    list_dmg = []
    list_num = []
    list_dmg1 = []
    list_num1 = []
    itogo_dmg=0
    itogo=0
    date_ret = request.values.get('date_return')

    print(list_dmg)
    if request.values.get('return_bike') and request.values.get('damage0'):
        for i in range(count_plus+2):
            list_dmg1.append(request.values.get('damage' + str(i)))
            list_num1.append(request.values.get('num' + str(i)))
            #print(len(list_dmg1))
        if len(list_dmg1) !=0:
            for i in range(len(list_dmg1)):
                df_type_dmg = get_dmg_type(conn, list_dmg1[i])
                #print(df_type_dmg)
                df_get_dmg = get_dmg_rented(conn, request.values.get('id_bike_return'), request.values.get('date_return'), request.values.get('idbike_rental'), list_num1[i], df_type_dmg.loc[0,'damage_name'], list_dmg1[i])

    #print(df_get_dmg)

    if request.values.get('id_bike_return'):
        id_bike=(request.values.get('id_bike_return'))

    if request.form.get('clear'):
        models=[]
        brands=[]
        types=[]
        years=[]
        table='get_tabl4'
    elif request.values.get('id_bike_rent'):
        models = request.form.getlist('Модель')
        brands = request.form.getlist('Бренд')
        types = request.form.getlist('Тип')
        years = request.form.getlist('Год_Выпуска')
        table = request.form.get('Наличие')
        for i in range(len(years)):
            years[i] = int(years[i])
        to_rent(conn, request.values.get('date_issue'),request.values.get('id_bike_rent'),request.values.get('client'))
    elif request.values.get('id_bike_return'):
        models = request.form.getlist('Модель')
        brands = request.form.getlist('Бренд')
        types = request.form.getlist('Тип')
        years = request.form.getlist('Год_Выпуска')
        table = request.form.get('Наличие')
        for i in range(len(years)):
            years[i] = int(years[i])
        for i in range(count_plus):
            list_dmg1.append(request.values.get('damage' + str(i)))
            list_num1.append(request.values.get('num' + str(i)))
        #print(list_dmg1)
        for i in range(len(list_dmg1)):
            df_type_dmg = get_dmg_type(conn, list_dmg1[i])
            #print(df_type_dmg)
        #get_rented(conn,request.values.get('id_bike_return'),request.values.get('date_return'))
        #print('bb')
    else:
        models=request.form.getlist('Модель')
        brands=request.form.getlist('Бренд')
        types=request.form.getlist('Тип')
        years=request.form.getlist('Год_Выпуска')
        table = request.form.get('Наличие')
        for i in range(len(years)):
            years[i] = int(years[i])

    if request.values.get('idbike_rental'):
        get_br = request.values.get('idbike_rental')

    df_pledge = get_pledge(conn, id_bike)
    df_pay_day = get_pay_day(conn, id_bike)
    df_date_issue = get_date_i(conn, id_bike)

    if request.values.get('confirm'):
        count_plus = int(request.values.get('count_dmg'))
        for i in range(count_plus + 1):
            list_dmg.append(request.values.get('damage' + str(i)))
            list_num.append(request.values.get('num' + str(i)))
        if request.values.get('date_return'):
            ret = datetime.strptime(request.values.get('date_return'), '%Y-%m-%d')
            iss = datetime.strptime(df_date_issue.loc[0, 'date_issue'], '%Y-%m-%d')
            dayS = (ret - iss).days

            pay_for_day = request.values.get('pay_check')
            for i in range(len(list_num)):
                if list_num[i]!='':
                    list_num[i] = int(list_num[i])
                    itogo_dmg = itogo_dmg + list_num[i]
                else:
                    break
            itogo = dayS * int(pay_for_day) + itogo_dmg
            print(itogo, '=--=-=')
    elif request.values.get('count_dmg'):
        count_plus = int(request.values.get('count_dmg')) + 1 + count_plus
        # print(count_plus)
        for i in range(count_plus):
            list_dmg.append(request.values.get('damage' + str(i)))
            list_num.append(request.values.get('num' + str(i)))
            # print(list_dmg)
            # print(str(i))

    df_damage = get_damage(conn)
    df_bike1 = get_bike(conn, id_bike)
    df_dmg_bike = get_dmg_for_bike(conn)
    df_get_one_dmg = get_one_dmg(conn,id_bike)
    df_dmg = df_dmg_bike['id_bike'].tolist()
    #print(df_dmg)



    df_br = get_bike_rentla(conn,id_bike)

    df_rent_bike = get_rent_bike(conn)
    df_info = get_info(conn, id_bike)

    df_client = get_client(conn)

    df_bike = get_bicycle(conn,models,brands,types,years,table)
    df_type = get_type(conn)
    df_brand = get_brand(conn)
    df_model = get_model(conn)
    df_year = get_year(conn)
    df_rent_bike = get_rent_bike(conn)

    df=df_rent_bike['id_b'].tolist()
    #print(df_dmg_bike)
    #print(df_damage)
    html = render_template(
        'index.html',
    bike=df_bike,
    list_df = [df_type, df_brand, df_model, df_year,df_table],
    sections = ['Тип','Бренд','Модель','Год_Выпуска','Наличие'],
    list_ch = [types,brands,models,years,table],
    rent_bike = df,
    info = df_info,
    check_rent = df,
    client = df_client,
    id_bike = id_bike,
    damage = df_damage,
    count_plus = count_plus,
    get_bike = df_bike1,
    list_dmg = list_dmg,
    list_num = list_num,
    clien = df_client,
    dmg_for_bike = df_dmg_bike,
    dmg_check = df_dmg,
    get_one_dmg = df_get_one_dmg,
    df_br = df_br,
    date_ret = date_ret,
    date_issue = df_date_issue,
    pay_day = df_pay_day,
    pledge = df_pledge,
    itogo = itogo,
    len=len,
    str = str,
    zip = zip)
    return html