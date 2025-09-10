#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NASA-weather-优化版本。想着一次性取2020-2025年间的数据，但是并发量太大成功率堪忧，换回一年可以运行
优化：多年循环 + 断点续抓 + 空数据保护 + 速度提升
"""
import io
import pandas as pd
import requests
import pymysql
import os
from datetime import date, timedelta
from tqdm import tqdm
from cities import CITIES          # 294 城表

# ---------- 配置 ----------
DB = dict(host='localhost', user='root', password='0000', db='weather_db', charset='utf8mb4', local_infile=True)
YEARS = list(range(2020, 2025))     # 2020-2024 并发量太大容易失去链接

OUT_DIR = 'nasa_history'            # 每年一个子目录
os.makedirs(OUT_DIR, exist_ok=True)

# ---------- 单年单城抓取 ----------
def fetch_year_city(city_id, name, lat, lng, year):
    """抓一年，返回 CSV 文本"""
    url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    params = {
        "start": f"{year}0101",
        "end": f"{year}1231",
        "latitude": lat,
        "longitude": lng,
        "community": "SB",          # SB 比 RE 宽松
        "parameters": "T2M_MAX,T2M_MIN,T2M",
        "format": "CSV",
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.text

# ---------- CSV → 规整 DataFrame ----------
def csv2df(csv_txt, city_id):
    """统一列名，返回 DataFrame"""
    lines = csv_txt.split('\n')
    data_lines = []
    for ln in lines[10:]:          # 跳过头 10 行注释
        if ln.strip() and not ln.startswith('#'):
            data_lines.append(ln)
    if not data_lines:
        return pd.DataFrame()      # 空表
    df = pd.read_csv(io.StringIO('\n'.join(data_lines)))
    # 只保留存在的列
    use_cols = ['YEAR', 'MO', 'DY', 'T2M_MAX', 'T2M_MIN', 'T2M']
    use_cols = [c for c in use_cols if c in df.columns]
    if len(use_cols) < 6:
        return pd.DataFrame()      # 列不齐不要
    df = df[use_cols]
    df['city_id'] = city_id
    df['date'] = pd.to_datetime(df[['YEAR', 'MO', 'DY']], format='%Y %j').dt.date
    df = df.rename(columns={
        'T2M_MAX': 'temp_max_c',
        'T2M_MIN': 'temp_min_c',
        'T2M': 'temp_avg_c'
    })[['city_id', 'date', 'temp_max_c', 'temp_min_c', 'temp_avg_c']]
    # 去掉缺测 -999
    df = df[(df[['temp_max_c', 'temp_min_c', 'temp_avg_c']] != -999).all(axis=1)]
    return df

# ---------- 断点续抓：已存在文件则跳过 ----------
def year_city2file(city_id, name, lat, lng, year):
    out_file = os.path.join(OUT_DIR, f'city_{city_id}_{year}.csv')
    if os.path.exists(out_file):
        return out_file          # 已抓，跳过
    df = csv2df(fetch_year_city(city_id, name, lat, lng, year), city_id)
    if df.empty:
        return None
    df.to_csv(out_file, index=False, date_format='%Y-%m-%d')
    return out_file

# ---------- 多年循环 + 合并 ----------
def main():
    print('Step 4-B：NASA 多年循环抓取 2020-2024')
    all_df = []
    for year in YEARS:
        print(f'\n===== 年份 {year} =====')
        for cid, (name, lat, lng) in tqdm(CITIES.items(), desc=f'{year} 城市进度'):
            f = year_city2file(cid, name, lat, lng, year)
            if f:
                all_df.append(pd.read_csv(f))
    if not all_df:
        print('无数据，退出')
        return
    final = pd.concat(all_df, ignore_index=True)
    all_csv = os.path.abspath('all_history.csv')
    final.to_csv(all_csv, index=False, date_format='%Y-%m-%d')
    print('合并完成，开始灌库 …')
    load2mysql(all_csv)

# ---------- LOAD DATA 灌库 ----------
def load2mysql(csv_path):
    # 先统一路径分隔符
    csv_path_unix = csv_path.replace('\\', '/')
    with pymysql.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.execute('SET FOREIGN_KEY_CHECKS = 0')
            cur.execute('TRUNCATE TABLE weather_daily')
            sql = """
            LOAD DATA LOCAL INFILE %s
            INTO TABLE weather_daily
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (city_id, @date_str, temp_max_c, temp_min_c, temp_avg_c)
            SET date = STR_TO_DATE(@date_str, '%Y-%m-%d')
            """
            cur.execute(sql, (csv_path_unix,))   # 参数化，避免转义
            cur.execute('SET FOREIGN_KEY_CHECKS = 1')
            cur.execute('SELECT COUNT(*) FROM weather_daily')
            cnt = cur.fetchone()[0]
            print(f'灌库完成！当前总记录：{cnt}')

if __name__ == '__main__':
    main()