#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NASA POWER 批量抓取 + LOAD DATA 灌库
· 含湿度/风速/PM2.5 
"""
import io
import pandas as pd
import requests
import pymysql
import os
import logging
from datetime import date
from tqdm import tqdm
from cities import CITIES

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nasa_weather.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- MySQL 设置 ----------
DB = dict(
    host='localhost', 
    user='root', 
    password='0000', 
    db='weather_db', 
    charset='utf8mb4',
    local_infile=True
)

# ---------- 时间 ----------
START = date(2024, 1, 1)         
END   = date(2024, 12, 31)  # 修正为完整年度

def get_valid_city_ids():
    """从数据库获取有效的city_id列表，用于数据过滤"""
    try:
        with pymysql.connect(**DB) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT city_id FROM city")
                return set(row[0] for row in cur.fetchall())
    except Exception as e:
        logger.error(f"获取有效城市ID失败: {str(e)}")
        return set()

# ---------- 单城抓取测试 ----------
def fetch_city_csv(city_id, name, lat, lng, start, end):
    """返回 CSV 文本"""
    url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    params = {
        "start": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "latitude": lat,
        "longitude": lng,
        "community": "RE",
        "parameters": "T2M_MAX,T2M_MIN,T2M",
        "format": "CSV",
    }
    try:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount('https://', adapter)
        r = session.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.text         
    except Exception as e:
        logger.error(f"抓取 {name} 失败: {str(e)}")
        return None

def city2file(city_id, name, lat, lng, start, end, out_dir, valid_city_ids):
    """保存为 city_{id}.csv，带 city_id 列，仅处理有效的城市ID"""
    # 检查城市ID是否在有效列表中
    if city_id not in valid_city_ids:
        logger.warning(f"城市ID {city_id} ({name}) 在city表中不存在，跳过处理")
        return None
        
    try:
        csv_txt = fetch_city_csv(city_id, name, lat, lng, start, end)
        if not csv_txt:
            return None
        
        # 分离注释和数据
        lines = csv_txt.split('\n')
        data_lines = []
        header_line = None
        
        for i, line in enumerate(lines):
            stripped_line = line.strip()
            if not stripped_line:
                continue
                
            if stripped_line.startswith('#'):
                continue
                
            if 'YEAR' in stripped_line and 'MO' in stripped_line and 'DY' in stripped_line:
                header_line = stripped_line
                data_lines = lines[i:]
                break
        
        if not header_line or not data_lines:
            logger.error(f"{name} 未找到有效数据行")
            return None
        
        clean_csv = '\n'.join(data_lines)
        df = pd.read_csv(io.StringIO(clean_csv))
        
        required_columns = ['YEAR', 'MO', 'DY', 'T2M_MAX', 'T2M_MIN', 'T2M']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.error(f"{name} 缺少必要的列 {missing}")
            return None
        
        # 过滤无效值（NASA缺测值通常为-999或-999.0）
        for col in required_columns[3:]:  # 只检查温度列
            df = df[df[col] != -999]
            df = df[df[col] != -999.0]
        
        if df.empty:
            logger.warning(f"{name} 过滤后无有效数据")
            return None
        
        df['city_id'] = city_id
        
        df['date'] = pd.to_datetime(
            df['YEAR'].astype(str) + '-' + 
            df['MO'].astype(str).str.zfill(2) + '-' + 
            df['DY'].astype(str).str.zfill(2)
        )
        
        df = df.rename(columns={
            'T2M_MAX': 'temp_max_c',
            'T2M_MIN': 'temp_min_c',
            'T2M': 'temp_avg_c'
        })
        
        # 确保温度列是浮点型
        for col in ['temp_max_c', 'temp_min_c', 'temp_avg_c']:
            df[col] = df[col].astype(float)
        
        df = df[['city_id', 'date', 'temp_max_c', 'temp_min_c', 'temp_avg_c']]
        
        out_file = os.path.join(out_dir, f'city_{city_id}.csv')
        df.to_csv(out_file, index=False, date_format='%Y-%m-%d')
        return out_file
        
    except Exception as e:
        logger.error(f"处理城市 {name} 时出错: {str(e)}")
        return None

# ---------- 批量抓 + 合并 ----------
def main():
    # 先获取数据库中有效的城市ID
    valid_city_ids = get_valid_city_ids()
    if not valid_city_ids:
        logger.error("无法获取有效城市ID列表，程序终止")
        return
        
    logger.info(f"已获取 {len(valid_city_ids)} 个有效的城市ID")
    
    out_dir = 'nasa_csv'
    os.makedirs(out_dir, exist_ok=True)
    logger.info(f'开始抓取 NASA POWER，{len(CITIES)} 城，{START} → {END}')
    
    success_count = 0
    for cid, (name, lat, lng) in tqdm(CITIES.items(), desc='城市进度'):
        if city2file(cid, name, lat, lng, START, END, out_dir, valid_city_ids):
            success_count += 1
    
    logger.info(f'抓取完成，成功处理 {success_count}/{len(CITIES)} 个城市')
    
    if success_count == 0:
        logger.warning('没有成功数据，取消后续操作')
        return
    
    logger.info('开始合并为 all.csv …')
    all_df = []
    for f in os.listdir(out_dir):
        if f.startswith('city_') and f.endswith('.csv'):
            try:
                all_df.append(pd.read_csv(os.path.join(out_dir, f)))
            except Exception as e:
                logger.error(f"读取 {f} 出错: {str(e)}")
    
    if all_df:
        final = pd.concat(all_df, ignore_index=True)
        # 保存时使用绝对路径，便于数据库导入
        all_csv_path = os.path.abspath('all.csv')
        # 提前处理路径中的反斜杠
        formatted_path = all_csv_path.replace('\\', '/')
        final.to_csv(all_csv_path, index=False, date_format='%Y-%m-%d')
        logger.info(f'合并完成，保存至 {all_csv_path}，开始 LOAD DATA 灌库 …')
        load2mysql(formatted_path)  # 传入处理后的路径
    else:
        logger.warning('没有可合并的数据，取消灌库操作')

def load2mysql(csv_path):
    try:
        df = pd.read_csv(csv_path)
        csv_count = len(df)
        logger.info(f"准备导入 {csv_count} 条记录到数据库...")
        
        # 数据预处理：确保没有空值
        for col in ['temp_max_c', 'temp_min_c', 'temp_avg_c']:
            if df[col].isnull().any():
                logger.warning(f"{col} 存在空值，已自动填充为0.0")
                df[col].fillna(0.0, inplace=True)
        
        with pymysql.connect(**DB) as conn:
            with conn.cursor() as cur:
                # 禁用外键检查，避免导入时约束错误
                cur.execute('SET FOREIGN_KEY_CHECKS = 0')
                cur.execute('TRUNCATE TABLE weather_daily')
                
                # 使用预先处理好的路径
                sql = f"""
                LOAD DATA LOCAL INFILE '{csv_path}'
                INTO TABLE weather_daily
                FIELDS TERMINATED BY ',' 
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
                (city_id, @date_str, @temp_max, @temp_min, @temp_avg)
                SET 
                    date = STR_TO_DATE(@date_str, '%Y-%m-%d'),
                    temp_max_c = CAST(@temp_max AS DECIMAL(5,2)),
                    temp_min_c = CAST(@temp_min AS DECIMAL(5,2)),
                    temp_avg_c = CAST(@temp_avg AS DECIMAL(5,2))
                """
                cur.execute(sql)
                
                # 恢复外键检查
                cur.execute('SET FOREIGN_KEY_CHECKS = 1')
                
                # 验证数量
                cur.execute("SELECT COUNT(*) FROM weather_daily")
                db_count = cur.fetchone()[0]
                logger.info(f"实际导入 {db_count} 条记录")
                
                if db_count != csv_count:
                    logger.warning(f"导入数量不匹配！CSV中有 {csv_count} 条，数据库中只有 {db_count} 条")
                    # 尝试方案2
                    logger.info("自动尝试方案2（批量插入）...")
                    batch_insert(df, conn, cur)
                else:
                    logger.info("数据导入数量匹配，验证通过")
            
            conn.commit()
            logger.info('灌库完成！')
            
    except Exception as e:
        logger.error(f"方案1失败: {str(e)}，尝试方案2（批量插入）...")
        batch_insert(df, conn if 'conn' in locals() else None)

def batch_insert(df, conn=None, cur=None):
    """批量插入作为备选方案"""
    try:
        csv_count = len(df)
        logger.info(f"准备批量插入 {csv_count} 条记录到数据库...")
        
        # 确保连接可用
        new_conn = False
        if not conn or not conn.open:
            conn = pymysql.connect(** DB)
            cur = conn.cursor()
            new_conn = True
            # 禁用外键检查
            cur.execute('SET FOREIGN_KEY_CHECKS = 0')
            cur.execute('TRUNCATE TABLE weather_daily')
        
        insert_sql = """
        INSERT INTO weather_daily 
        (city_id, date, temp_max_c, temp_min_c, temp_avg_c)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        success = 0
        failed = []
        batch_size = 500  # 减小批量大小
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            data = []
            for _, row in batch.iterrows():
                try:
                    data.append((
                        int(row.city_id),
                        row.date,
                        float(row.temp_max_c),
                        float(row.temp_min_c),
                        float(row.temp_avg_c)
                    ))
                except Exception as e:
                    failed.append(f"城市ID: {row.city_id}, 日期: {row.date}, 错误: {str(e)}")
            
            if data:
                success += cur.executemany(insert_sql, data)
        
        # 恢复外键检查
        cur.execute('SET FOREIGN_KEY_CHECKS = 1')
        conn.commit()
        logger.info(f"实际插入 {success} 条记录")
        
        if success != csv_count:
            logger.warning(f"插入数量不匹配！CSV中有 {csv_count} 条，成功插入 {success} 条")
            if failed:
                logger.warning(f"前5条失败记录：{failed[:5]}")
        else:
            logger.info("批量插入数量匹配，验证通过")
        
    except Exception as e:
        logger.error(f"批量插入失败: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if 'new_conn' in locals() and new_conn and conn and conn.open:
            conn.close()

if __name__ == "__main__":
    main()
