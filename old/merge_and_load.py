#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge + Load Data · 一键图形界面
双击即可：合并段文件 → 灌库 → 弹窗结果
"""
import pandas as pd
import glob
import io
import pymysql
import time
import os
import tkinter as tk
from tkinter import messagebox

# ---------- 配置 ----------
DB = dict(host='localhost', user='root', password='0000', db='weather_db', charset='utf8mb4')

# ---------- 合并段文件 ----------
def merge_segments():
    files = glob.glob('nasa_history_final/city_*.csv')
    if not files:
        return pd.DataFrame()
    # 动态跳头 + 字段对齐
    def safe_read(f):
        with open(f, encoding='utf-8') as fp:
            lines = fp.readlines()
        start = 0
        for i, ln in enumerate(lines):
            if ln.strip() and not ln.startswith('#') and ',' in ln and 'YEAR' in ln:
                start = i
                break
        data = ''.join(lines[start:])
        return pd.read_csv(io.StringIO(data))
    df = pd.concat([safe_read(f) for f in files], ignore_index=True)
    df = df[['YEAR', 'MO', 'DY', 'T2M_MAX', 'T2M_MIN', 'T2M']]  # 只保留存在的列
    df.to_csv('all_history_final.csv', index=False, date_format='%Y-%m-%d')
    return df

# ---------- 灌库 ----------
def load_to_mysql(df):
    start = time.time()
    csv_path = os.path.abspath('all_history_final.csv')
    with pymysql.connect(**DB) as conn:
        with conn.cursor() as cur:
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
            cur.execute(sql, (csv_path.replace('\\', '/'),))   # ← 参数化，无格式串
            cur.execute('SELECT COUNT(*) FROM weather_daily')
            cnt = cur.fetchone()[0]
    return cnt, time.time() - start

# ---------- 图形界面 ----------
def main():
    root = tk.Tk()
    root.withdraw()  # 不显示主窗口
    try:
        df = merge_segments()
        if df.empty:
            messagebox.showwarning("无数据", "没有找到段文件，请先运行抓取脚本。")
            return
        cnt, sec = load_to_mysql(df)
        messagebox.showinfo("完成", f"灌库成功！\n总记录：{cnt:,}\n耗时：{sec:.2f} 秒")
    except Exception as e:
        messagebox.showerror("错误", str(e))
    finally:
        root.destroy()

if __name__ == '__main__':
    main()