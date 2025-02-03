import pandas as pd
import logging
from prophet import Prophet
from sqlalchemy import create_engine, text
import sys
import pyodbc
import cx_Oracle
import re
from datetime import datetime, timedelta
import numpy as np
from urllib.parse import quote_plus
import concurrent.futures  # Для многопоточности

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#############################################################################
# 1. ПОДКЛЮЧЕНИЕ К SQL SERVER
#############################################################################

# Параметры подключения к SQL Server
driver = 'ODBC Driver 17 for SQL Server'
host = '10.10.120.6'
port = '1433'
database = 'eho_export2acc'
username = 'powerbi'
password = 'KOu%eVgq1'

# Формирование строки подключения для pyodbc
connection_string = (
    f'DRIVER={driver};'
    f'SERVER={host};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)

try:
    conn = pyodbc.connect(connection_string)
    logging.info("Подключение к SQL Server успешно установлено.")
except Exception as e:
    logging.error(f"Ошибка при подключении к базе данных SQL Server: {e}")
    sys.exit(1)

# Создание движка SQLAlchemy
engine = create_engine(f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver={driver}")

#############################################################################
# 2. ЗАГРУЗКА И ПРЕДВАРИТЕЛЬНАЯ ОБРАБОТКА CSV С ДАННЫМИ
#############################################################################

# Чтение CSV с мёртвым остатком
deadstock_data = pd.read_csv('deadstock_info_new.csv')

# Переименование столбцов
deadstock_data.rename(columns={
    'Gas_Station_Name': 'Gas_Station_Name',
    'City': 'City',
    'Branch': 'Branch',
    'ObjectCode': 'OBJECTCODE',
    'Tank_Number': 'Tank_Number',
    'Deadstock_Level': 'Level_cm',
    'Deadstock_Volume': 'Volume_liters',
    'Max_Level': 'Max_Level',
    'Max_Volume': 'Max_Volume'
}, inplace=True)

# Фильтрация по ветке "ВКО"
city_name = "ВКО"
city_data = deadstock_data[deadstock_data['Branch'] == city_name].copy()
if city_data.empty:
    logging.error(f"Нет данных для города (ветки) {city_name}.")
    sys.exit(1)

# Функция для извлечения номера резервуара и названия топлива
def extract_tank_and_fuel(tank_value):
    # Ищем шаблон "Резервуар 1", "Резервуар 2" и т.д.
    tank_match = re.search(r'Резервуар[ау]?\s*(\d+)', tank_value, re.IGNORECASE)
    tank_number = int(tank_match.group(1)) if tank_match else None
    # Ищем виды топлива
    fuel_match = re.search(r'(АИ-80|АИ-92|АИ-95|АИ-98|ДТ|ДТ-Л|ДТ-3-25|ДТ-3-32|СУГ|АИ-95-IMPORT|ДТЗ)',
                           tank_value, re.IGNORECASE)
    fuel_name = fuel_match.group(1).upper() if fuel_match else None
    return pd.Series({'TANK': tank_number, 'FuelType': fuel_name})

# Применяем функцию и убираем строки с пропусками
city_data[['TANK', 'FuelType']] = city_data['Tank_Number'].apply(extract_tank_and_fuel)
city_data.dropna(subset=['TANK', 'FuelType'], inplace=True)
city_data['TANK'] = city_data['TANK'].astype(int)

# Словарь соответствия FuelType -> GASNUM
fuel_mapping = {
    'АИ-80': '3300000000',
    'АИ-92': '3300000002',
    'АИ-95': '3300000005',
    'АИ-98': '3300000008',
    'ДТ': '3300000010',
    'ДТ-Л': '3300000010',
    'ДТ-3-25': '3300000029',
    'ДТ-3-32': '3300000038',
    'СУГ': '3400000000',
    'АИ-95-IMPORT': '3300000095',
    'ДТЗ': '3300000010'
}

logging.info(f"Количество строк в city_data после обработки: {len(city_data)}")
logging.info(f"Пример данных:\n{city_data[['OBJECTCODE', 'TANK', 'FuelType', 'Volume_liters']].head()}")

# Получаем уникальные комбинации для обработки
unique_combinations = city_data[['OBJECTCODE', 'TANK', 'FuelType']].drop_duplicates()
logging.info(f"Количество уникальных комбинаций для обработки: {len(unique_combinations)}")
logging.info(f"{unique_combinations}")

#############################################################################
# 3. ПОДКЛЮЧЕНИЕ К ORACLE
#############################################################################

oracle_username = "Alikhan"
oracle_password = "M0unt2024@"
oracle_host = "10.10.120.96"
oracle_port = "1521"
oracle_service_name = "ORCL"

# Кодирование пароля, если есть спецсимволы
oracle_password_encoded = quote_plus(oracle_password)

oracle_connection_string = (
    f"oracle+cx_oracle://{oracle_username}:{oracle_password_encoded}@"
    f"{oracle_host}:{oracle_port}/?service_name={oracle_service_name}"
)

try:
    oracle_engine = create_engine(oracle_connection_string)
    logging.info("Успешное подключение к Oracle базе данных через SQLAlchemy.")
except Exception as e:
    logging.error(f"Ошибка при создании SQLAlchemy движка для Oracle: {e}")
    sys.exit(1)

#############################################################################
# 4. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
#############################################################################

def prepare_in_clause(values, is_string=False):
    """
    Готовит список значений для подстановки в SQL-запрос.
    Если список пуст, возвращает None.
    """
    if not values:
        return None
    if is_string:
        return ','.join(f"'{v}'" for v in values)
    else:
        return ','.join(str(v) for v in values)

def find_similar_days(target_weekday, target_hour, data, window_size=7):
    similar_days = data[(data['weekday'] == target_weekday) & (data['ds'].dt.hour == target_hour)]
    if not similar_days.empty:
        rolling_mean = similar_days['RECEIPTS_VOLUME'].rolling(window=window_size, min_periods=1).mean().iloc[-1]
    else:
        rolling_mean = data['RECEIPTS_VOLUME'].mean()
    return rolling_mean

#############################################################################
# 5. ФОРМИРОВАНИЕ ПАРАМЕТРОВ ДЛЯ ЗАПРОСОВ
#############################################################################

params_list = []
for idx, row in unique_combinations.iterrows():
    object_code = row['OBJECTCODE']
    tank_number = int(row['TANK'])
    fuel_name = row['FuelType']
    gasnum = fuel_mapping.get(fuel_name)
    if not gasnum:
        logging.warning(f"Неизвестный вид топлива: {fuel_name}")
        continue
    params_list.append((object_code, gasnum, tank_number))

# Формируем списки для оператора IN
object_codes = list(set(p[0] for p in params_list))
gasnums = list(set(p[1] for p in params_list))
tanks = list(set(p[2] for p in params_list))

# Если хотя бы один из списков пуст — завершаем выполнение
if not object_codes or not gasnums or not tanks:
    logging.error("Параметры для фильтрации (object_codes, gasnums или tanks) пусты. Проверьте CSV и fuel_mapping.")
    sys.exit(1)

object_codes_str = prepare_in_clause(object_codes, is_string=True)
gasnums_str = prepare_in_clause(gasnums, is_string=True)
tanks_str = prepare_in_clause(tanks)

#############################################################################
# 6. ЗАПРОС К SQL SERVER
#############################################################################

query = f"""
SELECT
    CAST(r_day AS DATE) AS DATE,
    r_hour AS R_HOUR,
    objectcode AS OBJECTCODE,
    gasnum AS GASNUM,
    tank AS TANK,
    ISNULL(volume, 0) AS RECEIPTS_VOLUME
FROM ord_salesbyhour
WHERE
    objectcode IN ({object_codes_str})
    AND gasnum IN ({gasnums_str})
    AND tank IN ({tanks_str})
"""

try:
    df_all = pd.read_sql_query(query, con=engine)
    logging.info("Данные успешно загружены из ord_salesbyhour.")
except Exception as e:
    logging.error(f"Ошибка при чтении данных из SQL Server: {e}")
    sys.exit(1)

# Преобразуем поля даты и времени
df_all['DATE'] = pd.to_datetime(df_all['DATE'], errors='coerce')
df_all.loc[df_all['R_HOUR'] == 24, 'R_HOUR'] = 0
df_all.loc[df_all['R_HOUR'] == 0, 'DATE'] = df_all['DATE'] + pd.Timedelta(days=1)
df_all['ds'] = pd.to_datetime(df_all['DATE'].dt.strftime('%Y-%m-%d') + ' ' + df_all['R_HOUR'].astype(str) + ':00:00')
df_all['weekday'] = df_all['ds'].dt.weekday
df_all['GASNUM'] = df_all['GASNUM'].astype(str)

# Группируем по (OBJECTCODE, TANK, GASNUM)
grouped = df_all.groupby(['OBJECTCODE', 'TANK', 'GASNUM'])

#############################################################################
# 7. ЗАПРОС К ORACLE ДЛЯ ТЕКУЩИХ ОБЪЁМОВ
#############################################################################

oracle_query = f"""
SELECT t.OBJECTCODE, t.TANK, t.GASNUM, t.VOLUME AS VOLUME
FROM BI.tigmeasurements t
WHERE t.ID IN (
    SELECT MAX(ID)
    FROM BI.tigmeasurements
    WHERE TRUNC(POSTIMESTAMP) = TRUNC(SYSDATE)
    GROUP BY OBJECTCODE, TANK, GASNUM
)
AND t.OBJECTCODE IN ({object_codes_str})
AND t.TANK IN ({tanks_str})
AND t.GASNUM IN ({gasnums_str})
"""

try:
    oracle_df = pd.read_sql(oracle_query, con=oracle_engine)
    logging.info("Текущие объемы успешно загружены из Oracle.")
except Exception as e:
    logging.error(f"Ошибка при выполнении запроса к Oracle: {e}")
    sys.exit(1)

oracle_df.columns = [col.upper() for col in oracle_df.columns]
oracle_df['GASNUM'] = oracle_df['GASNUM'].astype(str)

#############################################################################
# 8. ОБРАБОТКА ДАННЫХ ДЛЯ ПРОГНОЗА (ФУНКЦИЯ process_combination)
#############################################################################

def process_combination(group_key):
    """
    Выполняет расчет прогноза для конкретной комбинации (OBJECTCODE, TANK, GASNUM).
    """
    object_code, tank_number, gasnum = group_key
    gasnum_str = str(gasnum)

    # Определяем название топлива по словарю fuel_mapping
    fuel_name = None
    for key, value in fuel_mapping.items():
        if value == gasnum_str:
            fuel_name = key
            break
    if not fuel_name:
        logging.warning(f"Не найден вид топлива для GASNUM {gasnum}")
        return None

    logging.info(f"Обработка OBJECTCODE={object_code}, TANK={tank_number}, FuelType={fuel_name}")

    # Получаем мертвый остаток из city_data
    dead_stock_row = city_data[
        (city_data['OBJECTCODE'] == object_code) &
        (city_data['TANK'] == tank_number) &
        (city_data['FuelType'] == fuel_name)
    ]
    if dead_stock_row.empty:
        logging.warning(f"Не найден мертвый остаток для OBJECTCODE={object_code}, TANK={tank_number}, FuelType={fuel_name}")
        return None
    dead_stock = dead_stock_row['Volume_liters'].values[0]

    # Получаем текущий объем из oracle_df
    current_volume_row = oracle_df[
        (oracle_df['OBJECTCODE'] == object_code) &
        (oracle_df['TANK'] == tank_number) &
        (oracle_df['GASNUM'] == gasnum_str)
    ]
    if current_volume_row.empty:
        logging.warning(f"Нет данных о текущем объеме для OBJECTCODE={object_code}, TANK={tank_number}, FuelType={fuel_name}")
        return None
    initial_volume = current_volume_row['VOLUME'].values[0]

    # Получаем исторические данные для группы
    try:
        df = grouped.get_group((object_code, tank_number, gasnum_str)).copy()
    except KeyError:
        logging.warning(f"Нет данных для группы: OBJECTCODE={object_code}, TANK={tank_number}, GASNUM={gasnum_str}")
        return None

    # Группируем данные по времени
    df = df.groupby('ds', as_index=False).agg({'RECEIPTS_VOLUME': 'sum', 'weekday': 'first'})
    start_date = df['ds'].min().normalize()
    end_date = df['ds'].max().normalize() + pd.Timedelta(days=1)
    complete_index = pd.date_range(start=start_date, end=end_date, freq='H')
    df = df.set_index('ds').reindex(complete_index).reset_index()
    df.rename(columns={'index': 'ds'}, inplace=True)
    df['weekday'] = df['weekday'].fillna(df['ds'].dt.weekday)
    df.set_index('ds', inplace=True)

    # Заполнение пропусков в RECEIPTS_VOLUME
    if df['RECEIPTS_VOLUME'].isnull().any():
        logging.warning(f"Есть пустые значения в 'RECEIPTS_VOLUME' для OBJECTCODE={object_code}, TANK={tank_number}, FuelType={fuel_name}. Заполняем средним.")
        df['RECEIPTS_VOLUME'] = df['RECEIPTS_VOLUME'].interpolate(method='time')
        df['RECEIPTS_VOLUME'] = df['RECEIPTS_VOLUME'].bfill().ffill()
    df.reset_index(inplace=True)

    historical_df = df[df['ds'] < pd.to_datetime(datetime.now())]
    prophet_df = historical_df[['ds', 'RECEIPTS_VOLUME']].rename(columns={'RECEIPTS_VOLUME': 'y'})
    prophet_df = prophet_df[prophet_df['y'] >= 0]

    # Проверка достаточности данных для обучения модели
    if len(prophet_df) < 24:
        logging.warning(f"Недостаточно данных для обучения модели Prophet для OBJECTCODE={object_code}, TANK={tank_number}, FuelType={fuel_name}")
        return None

    # Обучаем модель Prophet
    model = Prophet(
        seasonality_mode='additive',
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10.0
    )
    model.add_seasonality(name='weekly', period=7, fourier_order=3)
    model.add_seasonality(name='daily', period=1, fourier_order=5)
    model.add_seasonality(name='hourly', period=24, fourier_order=15)
    try:
        model.fit(prophet_df)
    except Exception as e:
        logging.error(f"Ошибка при обучении модели Prophet для OBJECTCODE={object_code}, TANK={tank_number}, FuelType={fuel_name}: {e}")
        return None

    # Прогноз на 1 день (2025-02-01)
    forecast_start_date = pd.to_datetime("2025-02-01 00:00:00")
    forecast_end_date = forecast_start_date + pd.Timedelta(hours=23)
    forecast_dates = pd.date_range(start=forecast_start_date, end=forecast_end_date, freq='H')
    future = pd.DataFrame({'ds': forecast_dates})
    forecast = model.predict(future)[['ds', 'yhat']]
    forecast['yhat'] = forecast['yhat'].clip(lower=0)

    # Скользящее среднее для сглаживания
    forecast['weekday'] = forecast['ds'].dt.weekday
    forecast['hour'] = forecast['ds'].dt.hour
    forecast['rolling_mean'] = forecast.apply(
        lambda row: find_similar_days(row['weekday'], row['hour'], historical_df),
        axis=1
    )
    forecast['combined_forecast'] = (0.3 * forecast['rolling_mean']) + (0.7 * forecast['yhat'])
    forecast['combined_forecast'] = forecast['combined_forecast'].clip(lower=0)
    forecast_values = forecast['combined_forecast'].tolist()

    # Формируем DataFrame с прогнозом
    forecast_df = pd.DataFrame({'ds': forecast_dates, 'yhat': forecast_values})
    forecast_df['DATE'] = forecast_df['ds'].dt.date
    forecast_df['current_volume'] = initial_volume - np.cumsum(forecast_df['yhat'])

    # Определяем момент достижения мертвого остатка
    below_dead_stock = forecast_df[forecast_df['current_volume'] <= dead_stock]
    if not below_dead_stock.empty:
        deadstock_date = below_dead_stock.iloc[0]['ds']
        logging.info(f"Мертвый остаток будет достигнут к: {deadstock_date}")
    else:
        deadstock_date = None
        logging.info("Мертвый остаток не будет достигнут в пределах заданного периода.")

    # Формируем итоговый DataFrame для вставки в базу
    forecast_df.reset_index(drop=True, inplace=True)
    forecast_df['objectcode'] = object_code
    forecast_df['gasnum'] = gasnum_str
    forecast_df['tank'] = tank_number
    forecast_df['date_time'] = forecast_df['ds']
    forecast_df['forecast_volume_sales'] = forecast_df['yhat']
    forecast_df['forecast_current_volume'] = forecast_df['current_volume']
    forecast_df['date_time_deadstock'] = deadstock_date

    insert_df = forecast_df[[
        'objectcode', 'gasnum', 'tank', 'date_time',
        'forecast_volume_sales', 'date_time_deadstock',
        'forecast_current_volume'
    ]]
    return insert_df

#############################################################################
# 9. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА КОМБИНАЦИЙ И ЗАПИСЬ РЕЗУЛЬТАТА В БАЗУ
#############################################################################

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(process_combination, grouped.groups.keys()))

# Отбираем ненулевые результаты
insert_dfs = [res for res in results if res is not None]
if insert_dfs:
    final_insert_df = pd.concat(insert_dfs, ignore_index=True)

    # Формируем даты для удаления старых записей
    forecast_date = pd.to_datetime("2025-02-01")
    forecast_start_str = forecast_date.strftime('%Y-%m-%d 00:00:00')
    forecast_end_str = forecast_date.strftime('%Y-%m-%d 23:59:59')

    # Удаляем старые записи за указанный период
    try:
        delete_query = f"""
        DELETE FROM ord_forecast
        WHERE
            objectcode IN ({object_codes_str})
            AND gasnum IN ({gasnums_str})
            AND tank IN ({tanks_str})
            AND date_time BETWEEN '{forecast_start_str}' AND '{forecast_end_str}'
        """
        with engine.begin() as connection:
            result = connection.execute(text(delete_query))
            deleted_count = result.rowcount
            logging.info(f"Удалено {deleted_count} записей из ord_forecast за {forecast_date.date()}.")
    except Exception as e:
        logging.error(f"Ошибка при удалении старых записей из базы данных: {e}")

    # Вставляем новые прогнозные данные
    try:
        final_insert_df.to_sql('ord_forecast', con=engine, if_exists='append', index=False)
        logging.info("Новые данные успешно вставлены в ord_forecast.")
    except Exception as e:
        logging.error(f"Ошибка при вставке данных в базу данных: {e}")
else:
    logging.info("Нет данных для вставки.")

# Освобождаем ресурсы
oracle_engine.dispose()
conn.close()
logging.info("Все операции завершены.")
