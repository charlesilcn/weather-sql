import re
from io import StringIO
from datetime import datetime
import pandas as pd  # 导入pandas库
import logging
import os
from config.config import SCRAPER_CONFIG

logger = logging.getLogger('nasa_weather_scraper')

def get_output_dir() -> str:
    """获取数据输出目录，若不存在则创建"""
    output_dir = SCRAPER_CONFIG['output_dir']
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # 确保目录存在
    return output_dir

def clean_nasa_data(csv_str: str, city_id: int) -> pd.DataFrame:
    """
    清洗NASA数据（增强版），处理列名变化
    
    Args:
        csv_str: CSV格式的字符串数据
        city_id: 城市ID
        
    Returns:
        清洗后的DataFrame，若清洗失败则返回空DataFrame
    """
    # 首先查看原始数据前20行，帮助诊断格式问题
    lines = csv_str.splitlines()
    if len(lines) > 20:
        logger.debug(f"城市ID {city_id} 的CSV前20行数据: \n" + "\n".join(lines[:20]))
    
    # 尝试解析日期范围行
    date_range_line = None
    for line in lines[:5]:
        if 'through' in line and ('month' in line or 'date' in line.lower()):
            date_range_line = line
            break
    
    if not date_range_line:
        logger.error(f"城市ID {city_id} 未找到日期范围信息")
        return pd.DataFrame()
    
    # 提取日期范围（使用正则表达式）
    try:
        date_pattern = r'\b\d{2}/\d{2}/\d{4}\b'
        dates = re.findall(date_pattern, date_range_line)
        
        if len(dates) != 2:
            raise ValueError(f"未找到两个日期，找到: {dates}")
            
        start_date_str, end_date_str = dates
        start_date = datetime.strptime(start_date_str, "%m/%d/%Y")
        end_date = datetime.strptime(end_date_str, "%m/%d/%Y")
    except Exception as e:
        logger.error(f"城市ID {city_id} 解析日期范围失败: {e}，原始行: {date_range_line}")
        return pd.DataFrame()
    
    # 尝试找到数据开始的行（更灵活的匹配）
    data_start_row = None
    for i, line in enumerate(lines):
        # 更灵活的关键词匹配
        lower_line = line.lower()
        if ('t2m' in lower_line or 'temp' in lower_line) and \
           ('year' in lower_line or 'mo' in lower_line or 'dy' in lower_line or 'day' in lower_line):
            data_start_row = i
            break
    
    if data_start_row is None:
        # 尝试默认跳过前5行（应急方案）
        data_start_row = 5
        logger.warning(f"城市ID {city_id} 未找到数据起始行，尝试跳过前5行")
    
    # 解析实际数据行
    try:
        df = pd.read_csv(
            StringIO(csv_str),
            skiprows=data_start_row,
            sep=',',
            on_bad_lines='skip',
            engine='python'  # 使用python引擎提高兼容性
        )
    except Exception as e:
        logger.error(f"城市ID {city_id} 解析数据失败: {e}")
        return pd.DataFrame()
    
    # 打印所有列名（关键调试信息）
    logger.debug(f"城市ID {city_id} 数据列名: {df.columns.tolist()}")
    
    # 定义可能的列名映射（考虑更多可能性）
    temp_mappings = {
        't2m_max': ['t2m_max', 't2mmax', 'max_temp', 'maximum_temperature'],
        't2m_min': ['t2m_min', 't2mmin', 'min_temp', 'minimum_temperature'],
        't2m': ['t2m', 'temp', 'temperature', 'avg_temp', 'average_temperature']
    }
    
    date_mappings = {
        'year': ['year', 'yr', 'y'],
        'month': ['mo', 'month', 'm'],
        'day': ['dy', 'day', 'd']
    }
    
    # 自动匹配列名
    column_mapping = {}
    lower_columns = [col.lower() for col in df.columns]
    
    # 匹配温度列
    for target, candidates in temp_mappings.items():
        for candidate in candidates:
            if candidate in lower_columns:
                idx = lower_columns.index(candidate)
                column_mapping[df.columns[idx]] = target
                break
    
    # 匹配日期列
    for target, candidates in date_mappings.items():
        for candidate in candidates:
            if candidate in lower_columns:
                idx = lower_columns.index(candidate)
                column_mapping[df.columns[idx]] = target
                break
    
    # 检查是否匹配到足够的列
    missing_temp = [t for t in temp_mappings if t not in column_mapping.values()]
    missing_date = [d for d in date_mappings if d not in column_mapping.values()]
    
    if missing_temp:
        logger.warning(f"城市ID {city_id} 缺少温度列: {missing_temp}")
    if missing_date:
        logger.error(f"城市ID {city_id} 缺少日期列: {missing_date}")
        return pd.DataFrame()
    
    # 重命名列
    try:
        df = df.rename(columns=column_mapping)
    except Exception as e:
        logger.error(f"城市ID {city_id} 列名重命名失败: {e}")
        return pd.DataFrame()
    
    # 添加城市ID
    df['city_id'] = city_id
    
    # 转换日期
    try:
        df['date'] = pd.to_datetime(
            df[['year', 'month', 'day']],
            errors='coerce'
        )
        df = df.dropna(subset=['date'])
    except Exception as e:
        logger.error(f"城市ID {city_id} 日期转换失败: {e}")
        return pd.DataFrame()
    
    # 过滤缺测值（考虑多种可能的缺测标记）
    for col in ['t2m_max', 't2m_min', 't2m']:
        if col in df.columns:
            # 常见的缺测标记
            missing_values = [-999, -9999, 'NaN', 'NA', '']
            df = df[~df[col].isin(missing_values)]
    
    # 选择需要的列
    final_columns = ['city_id', 'date'] + [col for col in ['t2m_max', 't2m_min', 't2m'] if col in df.columns]
    return df[final_columns]
