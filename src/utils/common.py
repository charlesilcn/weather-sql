import os
import logging
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.config import LOG_CONFIG

def init_logger():
    """初始化日志系统"""
    log_file = os.path.join(LOG_CONFIG['log_dir'], LOG_CONFIG['log_filename'])
    logging.basicConfig(
        level=getattr(logging, LOG_CONFIG['level']),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    return logging.getLogger('nasa_weather_scraper')

# 初始化全局日志对象
logger = init_logger()

def create_retry_session(retry_config):
    """创建带重试机制的HTTP会话"""
    retry = Retry(
        total=retry_config['total'],
        backoff_factor=retry_config['backoff_factor'],
        status_forcelist=retry_config['status_forcelist']
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount('https://', adapter)
    return session

def clean_nasa_data(df, city_id):
    """清洗NASA数据（统一列名、过滤缺测值）"""
    # 保留必要列
    required_cols = ['YEAR', 'MO', 'DY', 'T2M_MAX', 'T2M_MIN', 'T2M']
    valid_cols = [col for col in required_cols if col in df.columns]
    if len(valid_cols) < 6:
        logger.warning(f"城市ID {city_id} 数据列不完整，跳过")
        return pd.DataFrame()
    
    df = df[valid_cols].copy()
    
    # 添加城市ID和日期列
    df['city_id'] = city_id
    try:
        df['date'] = pd.to_datetime(
            df['YEAR'].astype(str) + '-' +
            df['MO'].astype(str).str.zfill(2) + '-' +
            df['DY'].astype(str).str.zfill(2)
        )
    except Exception as e:
        logger.error(f"城市ID {city_id} 日期转换失败: {e}")
        return pd.DataFrame()
    
    # 重命名列并过滤缺测值（-999为NASA缺测标记）
    df = df.rename(columns={
        'T2M_MAX': 'temp_max_c',
        'T2M_MIN': 'temp_min_c',
        'T2M': 'temp_avg_c'
    })[['city_id', 'date', 'temp_max_c', 'temp_min_c', 'temp_avg_c']]
    
    # 过滤缺测值
    df = df[(df[['temp_max_c', 'temp_min_c', 'temp_avg_c']] != -999).all(axis=1)]
    return df