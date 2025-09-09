#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NASA POWER Final · 多年全量分段重试
Dynamic header skip + Field alignment + Segment resume
2020-2024 full history · 294 cities · T2M only
"""
import io
import pandas as pd
import requests
import os
import time
import random
import logging
from datetime import date
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from cities import CITIES

# ---------- 配置 ----------
DB = dict(host='localhost', user='root', password='0000', db='weather_db', charset='utf8mb4', local_infile=True)
YEARS = list(range(2020, 2025))          # 2020-2024
OUT_DIR = 'nasa_history_final'           # 独立目录
os.makedirs(OUT_DIR, exist_ok=True)

# ---------- 重试 Session ----------
def new_session():
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount('https://', adapter)
    return s

# ---------- 单年分段抓取 ----------
def fetch_year_city(city_id, name, lat, lng, year):
    """每年拆 4 段，≈ 91 天/段，减少单请求体积"""
    segments = [
        (f"{year}0101", f"{year}0331"),
        (f"{year}0401", f"{year}0630"),
        (f"{year}0701", f"{year}0930"),
        (f"{year}1001", f"{year}1231")
    ]
    all_txt = []
    for seg_start, seg_end in segments:
        seg_file = os.path.join(OUT_DIR, f'city_{city_id}_{year}_{seg_start}.csv')
        if os.path.exists(seg_file):
            # 段已存在，直接读
            all_txt.append(open(seg_file, encoding='utf-8').read())
            continue
        # 段不存在，抓 + 重试
        for try_i in range(3):
            try:
                s = new_session()
                r = s.get(
                    "https://power.larc.nasa.gov/api/temporal/daily/point",
                    params={
                        "start": seg_start,
                        "end": seg_end,
                        "latitude": lat,
                        "longitude": lng,
                        "community": "SB",
                        "parameters": "T2M_MAX,T2M_MIN,T2M",
                        "format": "CSV",
                    },
                    timeout=30,
                )
                r.raise_for_status()
                all_txt.append(r.text)
                # 落盘断点
                with open(seg_file, "w", encoding='utf-8') as f:
                    f.write(r.text)
                break
            except Exception as e:
                logging.warning(f"{name} {seg_start}-{seg_end} try {try_i+1}: {e}")
                time.sleep(2 ** try_i + random.random())
        else:
            raise RuntimeError(f"{name} {year} 全段重试失败")
    return "\n".join(all_txt)

# ---------- CSV → 规整 DataFrame ----------
def csv2df(csv_txt, city_id):
    """动态跳头 + 字段对齐 + 缺测过滤"""
    lines = csv_txt.split('\n')
    # 1. 动态找数据起始行（跳过所有 # 和空行）
    data_start = 0
    for i, ln in enumerate(lines):
        if ln.strip() and not ln.startswith('#'):
            data_start = i
            break
    data_lines = [ln for ln in lines[data_start:] if ln.strip() and not ln.startswith('#')]
    if not data_lines:
        return pd.DataFrame()

    # 2. 只保留存在的列（免费版可能缺字段）
    df = pd.read_csv(io.StringIO('\n'.join(data_lines)))
    use_cols = ['YEAR', 'MO', 'DY', 'T2M_MAX', 'T2M_MIN', 'T2M']
    use_cols = [c for c in use_cols if c in df.columns]
    if len(use_cols) < 6:          # 列不齐不要
        return pd.DataFrame()

    df = df[use_cols]
    df['city_id'] = city_id
    df['date'] = pd.to_datetime(df[['YEAR', 'MO', 'DY']], format='%Y %j').dt.date
    df = df.rename(columns={
        'T2M_MAX': 'temp_max_c',
        'T2M_MIN': 'temp_min_c',
        'T2M': 'temp_avg_c'
    })[['city_id', 'date', 'temp_max_c', 'temp_min_c', 'temp_avg_c']]

    # 3. 去掉缺测 -999
    df = df[(df[['temp_max_c', 'temp_min_c', 'temp_avg_c']] != -999).all(axis=1)]
    return df
def csv2df(csv_txt, city_id):
    """动态跳头 + 字段对齐 + 缺测过滤"""
    lines = csv_txt.split('\n')
    # 1. 动态找数据起始行（跳过所有 # 和空行）
    data_start = 0
    for i, ln in enumerate(lines):
        if ln.strip() and not ln.startswith('#'):
            data_start = i
            break
    data_lines = [ln for ln in lines[data_start:] if ln.strip() and not ln.startswith('#')]
    if not data_lines:
        return pd.DataFrame()

    # 2. 首行必须是逗号分隔的表头（防止 HTML 错误页）
    header = data_lines[0]
    if ',' not in header or 'YEAR' not in header:
        # 拿到错误页或空文件
        return pd.DataFrame()

    # 3. 只保留存在的列（免费版可能缺字段）
    df = pd.read_csv(io.StringIO('\n'.join(data_lines)))
    use_cols = ['YEAR', 'MO', 'DY', 'T2M_MAX', 'T2M_MIN', 'T2M']
    use_cols = [c for c in use_cols if c in df.columns]
    if len(use_cols) < 6:          # 列不齐不要
        return pd.DataFrame()

    df = df[use_cols]
    df['city_id'] = city_id
    df['date'] = pd.to_datetime(df[['YEAR', 'MO', 'DY']], format='%Y %j').dt.date
    df = df.rename(columns={
        'T2M_MAX': 'temp_max_c',
        'T2M_MIN': 'temp_min_c',
        'T2M': 'temp_avg_c'
    })[['city_id', 'date', 'temp_max_c', 'temp_min_c', 'temp_avg_c']]

    # 4. 去掉缺测 -999
    df = df[(df[['temp_max_c', 'temp_min_c', 'temp_avg_c']] != -999).all(axis=1)]
    return df

# ---------- 多年循环 + 合并 ----------
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    print('Step 4-B：NASA POWER Final · 多年分段重试 2020-2024')
    all_df = []
    for year in YEARS:
        print(f'\n===== Year {year} =====')
        for cid, (name, lat, lng) in tqdm(CITIES.items(), desc=f'{year} Cities'):
            csv_txt = fetch_year_city(cid, name, lat, lng, year)
            if not csv_txt:
                continue
            df = csv2df(csv_txt, cid)
            if not df.empty:
                all_df.append(df)
    if not all_df:
        print('No data captured.')
        return
    final = pd.concat(all_df, ignore_index=True)
    all_csv = os.path.abspath('all_history_final.csv')
    final.to_csv(all_csv, index=False, date_format='%Y-%m-%d')
    print('Merge complete, start loading …')
    load2mysql(all_csv)

# ---------- LOAD DATA 灌库 ----------
def load2mysql(csv_path):
    csv_unix = csv_path.replace('\\', '/')
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
            cur.execute(sql, (csv_unix,))
            cur.execute('SET FOREIGN_KEY_CHECKS = 1')
            cur.execute('SELECT COUNT(*) FROM weather_daily')
            cnt = cur.fetchone()[0]
            print(f'Load complete! Total records: {cnt}')

if __name__ == '__main__':
    main()