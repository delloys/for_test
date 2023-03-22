import pandas as pd
import numpy as np

def get_pledge(conn,id_b):
    return pd.read_sql('''
    SELECT pledge FROM bike_rental
    JOIN client USING (id_client)
    JOIN bike USING (id_bike)
    JOIN ModelBicycle USING (id_model)
    JOIN BrandBicycle USING (id_brand)
    JOIN TypeBicycle USING (id_type)
    WHERE bike.id_model=ModelBicycle.id_model AND id_bike=:id_b AND date_return IS NULL;
        ''', conn, params={"id_b": id_b})

def get_pay_day(conn,id_b):
    return pd.read_sql('''
    SELECT price_type FROM bike_rental
    JOIN client USING (id_client)
    JOIN bike USING (id_bike)
    JOIN ModelBicycle USING (id_model)
    JOIN BrandBicycle USING (id_brand)
    JOIN TypeBicycle USING (id_type)
    WHERE bike.id_model=ModelBicycle.id_model AND id_bike=:id_b AND date_return IS NULL;
        ''', conn, params={"id_b": id_b})

def get_date_i(conn,id_b):
    return pd.read_sql('''
    SELECT date_issue FROM bike_rental
    JOIN client USING (id_client)
    JOIN bike USING (id_bike)
    JOIN ModelBicycle USING (id_model)
    JOIN BrandBicycle USING (id_brand)
    JOIN TypeBicycle USING (id_type)
    WHERE bike.id_model=ModelBicycle.id_model AND id_bike=:id_b AND date_return IS NULL;
        ''', conn, params={"id_b": id_b})

def get_bike_rentla(conn,id_bike):
    return pd.read_sql('''
    SELECT idbike_rental FROM bike_rental
    WHERE id_bike=:id_bike AND date_return IS NULL
    ''',conn,params={"id_bike":id_bike})

def get_dmg_type(conn,id_type):
    return pd.read_sql('''
    SELECT damage_name FROM damage_type
    WHERE id_damage=:id_d;
    ''',conn,params={"id_d":id_type})

#return_bike.html
def get_one_dmg(conn,id_bike):
    return pd.read_sql('''
        SELECT id_bike,description AS Описание,damage_sum AS Сумма_ущерба,date_issue AS Дата_выдачи,date_return AS Дата_Возврата FROM bike_rental
        JOIN rental_journal USING (idbike_rental)
        WHERE id_bike=:id_bike
        ''', conn,params={"id_bike":id_bike})

#return_bike.html
def get_dmg_for_bike(conn):
    return pd.read_sql('''
    SELECT id_bike,description,damage_sum FROM bike_rental
    JOIN rental_journal USING (idbike_rental)
    ''',conn)

#return_bike.html
def get_bike(conn,id_b):
    return pd.read_sql('''
    SELECT id_client AS id,full_name AS ФИО,phone_num AS Номер,id_bike,name_model AS Модель,brand_name AS Бренд,release_year AS Год_Выпуска,type_name AS Тип,price_type AS Цена_День,pledge AS Залог,date_issue AS Дата_выдачи FROM bike_rental
    JOIN client USING (id_client)
    JOIN bike USING (id_bike)
    JOIN ModelBicycle USING (id_model)
    JOIN BrandBicycle USING (id_brand)
    JOIN TypeBicycle USING (id_type)
    WHERE bike.id_model=ModelBicycle.id_model AND id_bike=:id_b AND date_return IS NULL;
        ''', conn, params={"id_b": id_b})

#return_bike.html
def get_damage(conn):
    return pd.read_sql('''
    SELECT id_damage,damage_name,damage_price FROM damage_type;
    ''',conn)

#rent.html / return_bike.html
def get_client(conn):
    return pd.read_sql('''
    SELECT id_client,full_name FROM client;
    ''',conn)

#view.html / rent.html
def get_info(conn,id_b):
    return pd.read_sql('''
    SELECT id_bike, name_model AS Модель,brand_name AS Бренд,release_year AS Год_Выпуска,type_name AS Тип,price_type AS Цена_День,pledge AS Залог FROM bike
        JOIN ModelBicycle USING (id_model)
        JOIN BrandBicycle USING (id_brand)
        JOIN TypeBicycle USING (id_type)
    WHERE bike.id_model=ModelBicycle.id_model
    AND bike.id_bike=:id
    ''',conn,params={"id":id_b})

#index.html
def get_rented(conn,id_bike,date_return):
    cur = conn.cursor()
    cur.execute('''
    UPDATE bike_rental SET date_return=:date_ret
    WHERE id_bike=:id_b and date_return IS NULL
    ''',{"id_b":id_bike,"date_ret":date_return})
    conn.commit()
    return True

#index.html
def get_dmg_rented(conn,id_b,date_return,id_br,damage_sum,descr,id_dmg):
    cur = conn.cursor()
    cur.execute('''
    UPDATE bike_rental SET date_return=:date_ret
    WHERE id_bike=:id_b and date_return IS NULL
    ''',{"id_b":id_b,"date_ret":date_return})
    conn.commit()
    cur.execute('''
        INSERT INTO rental_journal (damage_sum,description,id_damage,idbike_rental) VALUES
        (:damage_sum,:descr,:id_dmg,:id_br);
        ''', {"damage_sum":damage_sum,"descr":descr,"id_dmg":id_dmg,"id_br":id_br})
    conn.commit()
    return True

#index.html / view.html
def get_rent_bike(conn):
    return pd.read_sql('''
    WITH get_id(id_bke) AS (SELECT id_bike FROM bike_rental),

    get_tabl0(id_b,date_ret,da) AS 
    (SELECT bike.id_bike,date_return,IIF(bike.id_bike NOT IN get_id,'net','da') FROM bike
     LEFT JOIN  bike_rental
     ON bike_rental.id_bike = bike.id_bike
     WHERE bike_rental.id_bike IS NULL OR bike_rental.id_bike IS NOT NULL),
                
    --Велосипеды,которые вообще не брали
    get_tabl1(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NULL AND da='net'),
    
    --Велосипеды, которые вернули
    get_tabl2(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NOT NULL AND da='da'),
    
    --Велосипеды в аренде
    get_tabl3(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NULL AND da='da')
    
    SELECT id_b FROM get_tabl3
    ''',conn)

#index.html
def get_non_rent_bike(conn):
    return pd.read_sql('''
    SELECT id_bike AS Название FROM bike_rental WHERE date_return IS NOT NULL;
    ''',conn)
#index.html
def to_rent(conn,date,bike,client):
    cur = conn.cursor()
    cur.execute('''INSERT INTO bike_rental (date_issue,id_bike,id_client) VALUES (:date,:bike,:client);''', {"date": date,"bike":bike,"client":client})
    conn.commit()
    return True
#index.html
def get_bicycle(conn,m,b,t,y,table):
    if table is None:
        table='get_tabl4'
    return pd.read_sql(f'''   
    WITH get_id(id_bke) AS (SELECT id_bike FROM bike_rental),

    get_tabl0(id_b,date_ret,da) AS 
    (SELECT bike.id_bike,date_return,IIF(bike.id_bike NOT IN get_id,'net','da') FROM bike
     LEFT JOIN  bike_rental
     ON bike_rental.id_bike = bike.id_bike
     WHERE bike_rental.id_bike IS NULL OR bike_rental.id_bike IS NOT NULL),
                
    --Велосипеды,которые вообще не брали
    get_tabl1(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NULL AND da='net'),
    
    --Велосипеды, которые вернули
    get_tabl2(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NOT NULL AND da='da'),
    
    --Велосипеды в аренде
    get_tabl3(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NULL AND da='da'),
    
    --Все велосипеды
    get_tabl4(id_b) AS
    (SELECT id_b FROM get_tabl0),
    
    --Велосипеды не в аренде
    get_tabl5(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE id_b NOT IN get_tabl3 GROUP BY id_b)
    
    SELECT id_bike, name_model AS Модель,brand_name AS Бренд,release_year AS Год_Выпуска,type_name AS Тип,price_type AS Цена_День,pledge AS Залог FROM bike
        JOIN ModelBicycle USING (id_model)
        JOIN BrandBicycle USING (id_brand)
        JOIN TypeBicycle USING (id_type)
    WHERE bike.id_model=ModelBicycle.id_model
    AND (Модель IN ({str(m).strip('[]')}) OR {not m}) 
         AND (Бренд IN ({str(b).strip('[]')}) OR {not b}) 
         AND (Тип IN ({str(t).strip('[]')}) OR {not t})
         AND (release_year IN ({str(y).strip('[]')}) OR {not y})
         AND (id_bike IN {str(table).strip('[]')} OR {not table})
    ORDER BY id_bike
    ''',conn)
#index.html
def get_tabl(conn):
    table='get_tabl1'
    m=[]
    b=[]
    t=[]
    y=[]
    return pd.read_sql(f'''
    WITH get_id(id_bke) AS (SELECT id_bike FROM bike_rental),

    get_tabl0(id_b,date_ret,da) AS 
    (SELECT bike.id_bike,date_return,IIF(bike.id_bike NOT IN get_id,'net','da') FROM bike
     LEFT JOIN  bike_rental
     ON bike_rental.id_bike = bike.id_bike
     WHERE bike_rental.id_bike IS NULL OR bike_rental.id_bike IS NOT NULL),
                
    --Велосипеды,которые вообще не брали
    get_tabl1(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NULL AND da='net'),
    
    --Велосипеды, которые вернули
    get_tabl2(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NOT NULL AND da='da'),
    
    --Велосипеды в аренде
    get_tabl3(id_b) AS
    (SELECT id_b FROM get_tabl0 WHERE 
    date_ret IS NULL AND da='da')
    
    SELECT id_bike, name_model AS Модель,brand_name AS Бренд,release_year AS Год_Выпуска,type_name AS Тип,price_type AS Цена_День,pledge AS Залог FROM bike
        JOIN ModelBicycle USING (id_model)
        JOIN BrandBicycle USING (id_brand)
        JOIN TypeBicycle USING (id_type)
    WHERE bike.id_model=ModelBicycle.id_model
    AND (Модель IN ({str(m).strip('[]')}) OR {not m}) 
         AND (Бренд IN ({str(b).strip('[]')}) OR {not b}) 
         AND (Тип IN ({str(t).strip('[]')}) OR {not t})
         AND (release_year IN ({str(y).strip('[]')}) OR {not y})
         AND (id_bike IN {str(table).strip('[]')} OR {not table})
    ORDER BY id_bike
    ''',conn)
#index.html
def get_type(conn):
    return pd.read_sql('''
    SELECT id_type AS id,TypeBicycle.type_name AS Название, COUNT(type_name) AS Количество FROM TypeBicycle
        JOIN BrandBicycle USING (id_type)
        JOIN ModelBicycle USING (id_brand)
        JOIN bike USING (id_model)
    WHERE bike.id_model=ModelBicycle.id_model
    GROUP BY type_name;
    ''',conn)
#index.html
def get_brand(conn):
    return pd.read_sql('''
    SELECT id_brand AS id, brand_name AS Название, COUNT(brand_name) AS Количество FROM BrandBicycle
        JOIN ModelBicycle USING (id_brand)
        JOIN bike USING (id_model)
    WHERE bike.id_model=ModelBicycle.id_model
    GROUP BY brand_name;
    ''', conn)
#index.html
def get_model(conn):
    return pd.read_sql('''
    SELECT id_model AS id, name_model AS Название, COUNT(name_model) AS Количество FROM ModelBicycle
        JOIN bike USING (id_model)
    WHERE bike.id_model=ModelBicycle.id_model
    GROUP BY name_model;
    ''',conn)
#index.html
def get_year(conn):
    return pd.read_sql('''
    SELECT release_year AS Название, COUNT(release_year) AS Количество FROM ModelBicycle
    GROUP BY release_year;
    ''',conn)
#index.html
def get_maxmin_price(conn):
    return pd.read_sql('''
    SELECT MIN(price_type) AS Min, MAX(price_type) AS Max FROM TypeBicycle;
    ''',conn)
#index.html
def get_maxmin_pledge(conn):
    return pd.read_sql('''
    SELECT MIN(pledge) AS Min, MAX(pledge) AS Max FROM TypeBicycle;
    ''',conn)