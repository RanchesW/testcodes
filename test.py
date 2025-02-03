# Импорт необходимых модулей
import pandas as pd
import logging
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sqlalchemy import create_engine, text
import sys
import pyodbc
import cx_Oracle
import re
from datetime import datetime, timedelta
import numpy as np
from urllib.parse import quote_plus
import concurrent.futures  # для многопоточности

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

########################################################################
# 1. Параметры подключения и глобальные константы
########################################################################

# Параметры подключения к SQL Server
SQL_DRIVER = 'ODBC Driver 17 for SQL Server'
SQL_HOST = '10.10.120.6'
SQL_PORT = '1433'
SQL_DATABASE = 'eho_export2acc'
SQL_USERNAME = 'powerbi'
SQL_PASSWORD = 'KOu%eVgq1'

# Создание строки подключения для pyodbc
connection_string = (
    f'DRIVER={SQL_DRIVER};'
    f'SERVER={SQL_HOST};'
    f'DATABASE={SQL_DATABASE};'
    f'UID={SQL_USERNAME};'
    f'PWD={SQL_PASSWORD}'
)

# Настраиваем SQLAlchemy
try:
    engine = create_engine(
        f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DATABASE}?driver={SQL_DRIVER}"
    )
    logging.info("Подключение к SQL Server успешно установлено.")
except Exception as e:
    logging.error(f"Ошибка при создании движка SQLAlchemy для SQL Server: {e}")
    sys.exit(1)

# Параметры подключения к Oracle
oracle_username = "Alikhan"
oracle_password = "M0unt2024@"
oracle_host = "10.10.120.96"
oracle_port = "1521"
oracle_service_name = "ORCL"

# Кодируем пароль, если есть специальные символы
oracle_password_encoded = quote_plus(oracle_password)

oracle_connection_string = (
    f"oracle+cx_oracle://{oracle_username}:{oracle_password_encoded}@{oracle_host}:{oracle_port}/?service_name={oracle_service_name}"
)

try:
    oracle_engine = create_engine(oracle_connection_string)
    logging.info("Успешное подключение к Oracle базе данных через SQLAlchemy.")
except Exception as e:
    logging.error(f"Ошибка при создании SQLAlchemy движка для Oracle: {e}")
    sys.exit(1)

########################################################################
# 2. Глобальные параметры прогноза и вспомогательные функции
########################################################################

# Даты начала и конца периода прогноза
FORECAST_START = "2024-11-01"
FORECAST_END = "2024-11-30 23:59:59"

def prepare_in_clause(values, is_string=False):
    """
    Готовит список значений для подстановки в SQL IN(...).
    """
    if is_string:
        return ','.join(f"'{v}'" for v in values)
    else:
        return ','.join(str(v) for v in values)

########################################################################
# 3. Загрузка CSV c "мертвыми остатками" и предварительная обработка
########################################################################

def load_and_filter_deadstock(city_name="ВКО"):
    """
    Читает CSV с обновлённой структурой мёртвых остатков (deadstock_info_new.csv),
    фильтрует по нужной ветке (по умолчанию ВКО),
    извлекает номер резервуара и тип топлива.
    Возвращает DataFrame city_data с колонками [OBJECTCODE, TANK, FuelType, Volume_liters].
    """
    deadstock_data = pd.read_csv('deadstock_info_new.csv')

    # Переименование столбцов (если нужно)
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

    # Фильтрация по ветке
    city_data = deadstock_data[deadstock_data['Branch'] == city_name].copy()
    if city_data.empty:
        logging.error(f"Нет данных для Branch={city_name} в CSV. Прерываем выполнение.")
        sys.exit(1)

    # Функция для извлечения номера резервуара и типа топлива
    def extract_tank_and_fuel(tank_value):
        # Ищем "Резервуар 1", "Резервуар 2" и т. п. 
        # Внутри строки может быть "Резервуар", "Резервуара", "Резервуару" и т. д.
        tank_match = re.search(r'Резервуар[ау]?\s*(\d+)', str(tank_value), re.IGNORECASE)
        if tank_match:
            tank_number = int(tank_match.group(1))
        else:
            tank_number = None

        # Ищем тип топлива
        fuel_match = re.search(
            r'(АИ-80|АИ-92|АИ-95|АИ-98|ДТ-Л|ДТ-3-25|ДТ-3-32|АИ-95-IMPORT|ДТЗ|ДТ|СУГ)',
            str(tank_value),
            re.IGNORECASE
        )
        if fuel_match:
            fuel_name = fuel_match.group(1).upper()
        else:
            fuel_name = None

        return pd.Series({'TANK': tank_number, 'FuelType': fuel_name})

    city_data[['TANK', 'FuelType']] = city_data['Tank_Number'].apply(extract_tank_and_fuel)

    # Удаляем строки с отсутствующими TANK или FuelType
    city_data.dropna(subset=['TANK', 'FuelType'], inplace=True)

    # Приводим TANK к int (если не получилось автоматически)
    city_data['TANK'] = city_data['TANK'].astype(int)

    return city_data

########################################################################
# 4. Новый улучшенный процесс расчёта прогноза
########################################################################

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

def process_combination_improved(group_key, grouped, oracle_df, city_data):
    """
    Улучшенная версия обработки прогноза для конкретного (OBJECTCODE, TANK, GASNUM).
    Добавлены:
    - фильтрация по 3 месяцам истории
    - удаление выбросов методом IQR
    - кросс-валидация Prophet
    - учитываются праздники (RU + KZ)
    - прогноз берётся из yhat_upper с дополнительным +5%
    - нижний порог спроса и округление вверх
    """
    object_code, tank_number, gasnum = group_key
    gasnum_str = str(gasnum)

    # Определяем FuelType (чтобы получить dead_stock)
    possible_fuels = [k for k, v in fuel_mapping.items() if v == gasnum_str]
    if not possible_fuels:
        logging.warning(f"Не найден вид топлива для GASNUM={gasnum_str}")
        return None
    fuel_name = possible_fuels[0]

    # Получаем dead_stock
    dead_stock_row = city_data[
        (city_data['OBJECTCODE'] == object_code) &
        (city_data['TANK'] == tank_number) &
        (city_data['FuelType'] == fuel_name)
    ]
    if dead_stock_row.empty:
        logging.warning(
            f"Не найден мертвый остаток для OBJECTCODE={object_code}, "
            f"TANK={tank_number}, FuelType={fuel_name}"
        )
        return None

    dead_stock = dead_stock_row['Volume_liters'].values[0]

    # Текущий объём из Oracle
    current_volume_row = oracle_df[
        (oracle_df['OBJECTCODE'] == object_code) &
        (oracle_df['TANK'] == tank_number) &
        (oracle_df['GASNUM'] == gasnum_str)
    ]
    if current_volume_row.empty:
        logging.warning(
            f"Нет текущего объёма в Oracle для OBJECTCODE={object_code}, "
            f"TANK={tank_number}, GASNUM={gasnum_str}"
        )
        return None

    initial_volume = current_volume_row['VOLUME'].values[0]

    # Берём исторические данные
    df = grouped.get_group((object_code, tank_number, gasnum_str)).copy()

    # Фильтруем историю: минимум 3 месяца до FORECAST_START
    min_history_date = pd.to_datetime(FORECAST_START) - pd.DateOffset(months=3)
    df = df[df['ds'] >= min_history_date]

    # Проверяем, что данных достаточно (хотя бы 24*30*3 = 2160 строк)
    if len(df) < 24 * 30 * 3:
        logging.warning(
            f"Недостаточно данных (меньше 3 мес.) для OBJECTCODE={object_code}, "
            f"TANK={tank_number}, GASNUM={gasnum_str}"
        )
        return None

    # Удаление выбросов методом IQR
    q1 = df['RECEIPTS_VOLUME'].quantile(0.25)
    q3 = df['RECEIPTS_VOLUME'].quantile(0.75)
    iqr = q3 - q1
    df = df[
        (df['RECEIPTS_VOLUME'] >= (q1 - 1.5*iqr)) &
        (df['RECEIPTS_VOLUME'] <= (q3 + 1.5*iqr))
    ]

    # Подготовим DataFrame для Prophet
    prophet_df = df[['ds', 'RECEIPTS_VOLUME']].rename(columns={'RECEIPTS_VOLUME': 'y'})
    prophet_df = prophet_df[prophet_df['y'] >= 0]

    # Модель Prophet
    model = Prophet(
        seasonality_mode='multiplicative',
        yearly_seasonality=False,
        weekly_seasonality=True,
        daily_seasonality=True,
        changepoint_prior_scale=0.15,
        interval_width=0.80
    )

    # Добавляем праздники
    model.add_country_holidays(country_name='RU')
    model.add_country_holidays(country_name='KZ')

    # Почасовая сезонность
    model.add_seasonality(
        name='hourly',
        period=1,
        fourier_order=12,
        prior_scale=15.0
    )

    # Обучаем модель и проводим кросс-валидацию
    try:
        model.fit(prophet_df)
        df_cv = cross_validation(
            model,
            initial='60 days',
            period='15 days',
            horizon='30 days'
        )
        df_p = performance_metrics(df_cv)
        mape = df_p['mape'].mean()
        logging.info(
            f"OBJECTCODE={object_code}, TANK={tank_number}, "
            f"GASNUM={gasnum_str}: MAPE={mape}"
        )
    except Exception as e:
        logging.error(
            f"Ошибка кросс-валидации Prophet для OBJECTCODE={object_code}, "
            f"TANK={tank_number}, GASNUM={gasnum_str}: {e}"
        )
        return None

    # Генерируем будущее на 30 дней (до конца ноября 2024)
    future = model.make_future_dataframe(
        periods=24*30,  # 30 дней почасово
        freq='H',
        include_history=False
    )
    forecast = model.predict(future)

    # Смещаем прогноз к верхним границам (yhat_upper) + 5%
    forecast['yhat'] = forecast['yhat_upper'] * 1.05

    # Округляем вверх
    forecast['yhat'] = np.ceil(forecast['yhat'])

    # Минимум 5% от среднего исторического
    avg_volume = prophet_df['y'].mean()
    min_volume = avg_volume * 0.05
    forecast['yhat'] = forecast['yhat'].clip(lower=min_volume)

    # Расчёт динамики текущего объёма
    forecast['cumulative_sales'] = forecast['yhat'].cumsum()
    forecast['current_volume'] = initial_volume - forecast['cumulative_sales']

    # Определяем дату, когда достигнет dead_stock
    below_deadstock = forecast[forecast['current_volume'] <= dead_stock]
    if not below_deadstock.empty:
        first_deadstock = below_deadstock.iloc[0]
        deadstock_date = first_deadstock['ds']
        logging.info(
            f"OBJECTCODE={object_code}, TANK={tank_number}, GASNUM={gasnum_str}: "
            f"DeadStock={dead_stock} будет достигнут {deadstock_date}"
        )
    else:
        deadstock_date = None

    # Подготовка к вставке
    insert_df = pd.DataFrame()
    insert_df['objectcode'] = [object_code]*len(forecast)
    insert_df['gasnum'] = [gasnum_str]*len(forecast)
    insert_df['tank'] = [tank_number]*len(forecast)
    insert_df['date_time'] = forecast['ds']
    insert_df['forecast_volume_sales'] = forecast['yhat']
    insert_df['forecast_current_volume'] = forecast['current_volume']
    insert_df['date_time_deadstock'] = deadstock_date

    return insert_df

########################################################################
# 5. Основной блок: чтение данных, группировка, параллельная обработка
########################################################################

def main():
    # Шаг 1: Загрузка и подготовка deadstock
    city_data = load_and_filter_deadstock(city_name="ВКО")
    logging.info(f"Количество строк после load_and_filter_deadstock: {len(city_data)}")

    # Генерация уникальных комбинаций
    unique_combinations = city_data[['OBJECTCODE', 'TANK', 'FuelType']].drop_duplicates()
    logging.info(f"unique_combinations.shape = {unique_combinations.shape}")

    # Подготовка списков для IN(..)
    params_list = []
    for idx, row in unique_combinations.iterrows():
        object_code = row['OBJECTCODE']
        tank_number = int(row['TANK'])
        fuel_name = row['FuelType']
        gasnum = fuel_mapping.get(fuel_name)
        if not gasnum:
            logging.warning(f"Для FuelType='{fuel_name}' не найден GASNUM в словаре fuel_mapping.")
            continue
        params_list.append((object_code, gasnum, tank_number))

    object_codes = list(set([p[0] for p in params_list]))
    gasnums = list(set([p[1] for p in params_list]))
    tanks = list(set([p[2] for p in params_list]))

    # ВАЖНАЯ ПРОВЕРКА НА ПУСТОТУ
    if not object_codes or not gasnums or not tanks:
        logging.error("Списки параметров (object_codes/gasnums/tanks) оказались пустыми. Прерываем выполнение.")
        sys.exit(1)

    object_codes_str = prepare_in_clause(object_codes, is_string=True)
    gasnums_str = prepare_in_clause(gasnums, is_string=True)
    tanks_str = prepare_in_clause(tanks)

    logging.info(f"object_codes_str: {object_codes_str}")
    logging.info(f"gasnums_str: {gasnums_str}")
    logging.info(f"tanks_str: {tanks_str}")

    # Шаг 2: Читаем данные из SQL Server только до FORECAST_START
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
        CAST(r_day AS DATE) <= '{FORECAST_START}'
        AND objectcode IN ({object_codes_str})
        AND gasnum IN ({gasnums_str})
        AND tank IN ({tanks_str})
    """

    try:
        df_all = pd.read_sql_query(query, con=engine)
        logging.info(f"Данные успешно загружены из ord_salesbyhour. Получено {len(df_all)} строк.")
    except Exception as e:
        logging.error(f"Ошибка при чтении данных из SQL Server: {e}")
        sys.exit(1)

    # Преобразуем DATE + R_HOUR -> datetime
    df_all['DATE'] = pd.to_datetime(df_all['DATE'], errors='coerce')
    df_all.loc[df_all['R_HOUR'] == 24, 'R_HOUR'] = 0
    df_all.loc[df_all['R_HOUR'] == 0, 'DATE'] = df_all['DATE'] + pd.Timedelta(days=1)

    df_all['ds'] = pd.to_datetime(
        df_all['DATE'].dt.strftime('%Y-%m-%d') + ' ' + df_all['R_HOUR'].astype(str) + ':00:00'
    )

    # Группируем
    df_all['weekday'] = df_all['ds'].dt.weekday
    df_all['GASNUM'] = df_all['GASNUM'].astype(str)

    grouped = df_all.groupby(['OBJECTCODE', 'TANK', 'GASNUM'])

    # Шаг 3: Загрузка текущих объёмов из Oracle
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
        logging.info(f"Текущие объемы успешно загружены из Oracle. Получено {len(oracle_df)} строк.")
    except Exception as e:
        logging.error(f"Ошибка при чтении данных из Oracle: {e}")
        sys.exit(1)

    oracle_df.columns = [col.upper() for col in oracle_df.columns]
    oracle_df['GASNUM'] = oracle_df['GASNUM'].astype(str)

    # Шаг 4: Параллельный расчёт
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for combo in grouped.groups.keys():
            futures.append(executor.submit(
                process_combination_improved,
                combo, grouped, oracle_df, city_data
            ))
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            if res is not None:
                results.append(res)

    if not results:
        logging.warning("Нет данных для вставки (все комбинации отфильтрованы или нет прогнозов).")
        return

    final_insert_df = pd.concat(results, ignore_index=True)
    logging.info(f"Сформировано {len(final_insert_df)} строк для вставки в таблицу ord_forecast.")

    # Шаг 5: Удаляем старые записи за ноябрь 2024
    forecast_start_str = pd.to_datetime(FORECAST_START).strftime('%Y-%m-%d 00:00:00')
    forecast_end_str = pd.to_datetime(FORECAST_END).strftime('%Y-%m-%d 23:59:59')

    delete_query = f"""
    DELETE FROM ord_forecast
    WHERE
        objectcode IN ({object_codes_str})
        AND gasnum IN ({gasnums_str})
        AND tank IN ({tanks_str})
        AND date_time BETWEEN '{forecast_start_str}' AND '{forecast_end_str}'
    """

    try:
        with engine.begin() as connection:
            result = connection.execute(text(delete_query))
            deleted_count = result.rowcount
            logging.info(
                f"Удалено {deleted_count} записей из ord_forecast за период "
                f"{FORECAST_START} - {FORECAST_END}."
            )
    except Exception as e:
        logging.error(f"Ошибка при удалении старых записей: {e}")

    # Вставка новых прогнозов
    try:
        final_insert_df.to_sql('ord_forecast', con=engine, if_exists='append', index=False)
        logging.info("Новые данные успешно вставлены в ord_forecast.")
    except Exception as e:
        logging.error(f"Ошибка при вставке данных в ord_forecast: {e}")

    # Закрываем соединения
    oracle_engine.dispose()
    logging.info("Все операции завершены.")

if __name__ == '__main__':
    main()
