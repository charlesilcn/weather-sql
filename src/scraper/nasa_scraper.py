import os
import time
import random
import pandas as pd
from io import StringIO
from config.config import SCRAPER_CONFIG, NASA_API_CONFIG
from src.utils.common import logger, create_retry_session, clean_nasa_data
from config.cities import CITIES  #城市数据地址

def fetch_city_segment(city_id, name, lat, lng, start_date, end_date):
    """抓取单个城市的一段日期数据（支持断点续抓）"""
    # 断点续抓：检查本地是否已存在该段数据
    seg_filename = f"city_{city_id}_{start_date}_{end_date}.csv"
    seg_path = os.path.join(SCRAPER_CONFIG['output_dir'], seg_filename)
    if os.path.exists(seg_path):
        logger.info(f"城市 {name}（{city_id}）{start_date}-{end_date} 已存在，跳过抓取")
        with open(seg_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    # 发送请求（带重试）
    session = create_retry_session(SCRAPER_CONFIG['retry'])
    for try_num in range(3):  # 额外重试3次（配合session的重试机制）
        try:
            response = session.get(
                url=NASA_API_CONFIG['url'],
                params={
                    'start': start_date,
                    'end': end_date,
                    'latitude': lat,
                    'longitude': lng,
                    'community': NASA_API_CONFIG['community'],
                    'parameters': NASA_API_CONFIG['parameters'],
                    'format': NASA_API_CONFIG['format']
                },
                timeout=SCRAPER_CONFIG['timeout']
            )
            response.raise_for_status()  # 触发HTTP错误（如404/500）
            # 保存数据到本地（断点续抓用）
            with open(seg_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            logger.debug(f"城市 {name}（{city_id}）{start_date}-{end_date} 抓取成功")
            return response.text
        except Exception as e:
            logger.warning(
                f"城市 {name}（{city_id}）{start_date}-{end_date} 第{try_num+1}次失败: {e}"
            )
            time.sleep(2 ** try_num + random.random())  # 指数退避
    logger.error(f"城市 {name}（{city_id}）{start_date}-{end_date} 抓取失败")
    return None

def fetch_city_year(city_id, name, lat, lng, year):
    """抓取单个城市一整年的数据（按季度分段）"""
    segments = [
        (f"{year}0101", f"{year}0331"),  # Q1
        (f"{year}0401", f"{year}0630"),  # Q2
        (f"{year}0701", f"{year}0930"),  # Q3
        (f"{year}1001", f"{year}1231")   # Q4
    ]
    all_data = []
    for start, end in segments:
        seg_data = fetch_city_segment(city_id, name, lat, lng, start, end)
        if not seg_data:
            continue
        # 解析CSV数据
        #lines = seg_data.split('\n')
        # 跳过注释行（动态查找数据起始行）
        #data_lines = [line for line in lines if line.strip() and not line.startswith('#')]
        #if not data_lines:
        #    logger.warning(f"城市 {name}（{city_id}）{start}-{end} 无有效数据")
        #    continue
        # 转换为DataFrame
        #try:
        #    df = pd.read_csv(StringIO('\n'.join(data_lines)))
        #    all_data.append(df)
        #except Exception as e:
        #    logger.error(f"城市 {name}（{city_id}）{start}-{end} 解析失败: {e}")
        lines = seg_data.split('\n')
        header_line_idx = None  # 表头行的索引
        data_start_idx = None   # 数据行的起始索引

        # 1. 遍历所有行，找到表头行（包含关键字 YEAR 和 T2M_MAX）
        for idx, line in enumerate(lines):
            line_stripped = line.strip()
            # 跳过空行和注释行
            if not line_stripped or line_stripped.startswith('#'):
                continue
            # 表头行必须包含 YEAR、MO、T2M_MAX（NASA CSV 固定表头）
            if 'YEAR' in line_stripped and 'MO' in line_stripped and 'T2M_MAX' in line_stripped:
                header_line_idx = idx
                data_start_idx = idx + 1  # 数据行从表头下一行开始
                break

        # 2. 检查是否找到有效表头
        if header_line_idx is None:
            logger.warning(f"城市 {name}（{city_id}）{start}-{end} 未找到有效表头，跳过")
            continue

        # 3. 提取表头 + 数据行（确保列数匹配）
        header_line = lines[header_line_idx].strip()
        data_lines = [line.strip() for line in lines[data_start_idx:] if line.strip()]  # 过滤数据行中的空行

        # 4. 合并表头和数据行，生成完整的CSV字符串
        csv_str = header_line + '\n' + '\n'.join(data_lines)
        if len(data_lines) == 0:
            logger.warning(f"城市 {name}（{city_id}）{start}-{end} 无数据行，跳过")
            continue

        # 5. 解析CSV（此时列数必然匹配）
        try:
            df = pd.read_csv(StringIO(csv_str))
            # 额外校验：确保必要列存在（防止API返回格式变更）
            required_cols = ['YEAR', 'MO', 'DY', 'T2M_MAX', 'T2M_MIN', 'T2M']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"城市 {name}（{city_id}）{start}-{end} 缺少必要列: {missing_cols}，跳过")
                continue
            all_data.append(df)
            logger.debug(f"城市 {name}（{city_id}）{start}-{end} 解析成功，共 {len(df)} 条数据")
        except Exception as e:
            logger.error(f"城市 {name}（{city_id}）{start}-{end} 解析失败: {e}", exc_info=True)
    if not all_data:
        return pd.DataFrame()
    # 合并全年数据并清洗
    year_df = pd.concat(all_data, ignore_index=True)
    return clean_nasa_data(year_df, city_id)

def fetch_all_cities():
    """抓取所有城市的多年数据"""
    all_df = []
    for year in SCRAPER_CONFIG['years']:
        logger.info(f"开始抓取 {year} 年数据")
        for city_id, (name, lat, lng) in CITIES.items():
            try:
                city_df = fetch_city_year(city_id, name, lat, lng, year)
                if not city_df.empty:
                    all_df.append(city_df)
                    logger.info(f"城市 {name}（{city_id}）{year} 年数据处理完成，共 {len(city_df)} 条")
            except Exception as e:
                logger.error(f"城市 {name}（{city_id}）{year} 年处理失败: {e}")
                continue
    if not all_df:
        logger.warning("未抓取到任何有效数据")
        return None
    # 合并所有数据并去重
    final_df = pd.concat(all_df, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=['city_id', 'date'], keep='last')
    logger.info(f"所有数据抓取完成，共 {len(final_df)} 条（去重后）")
    # 保存到总数据文件
    final_path = os.path.join(os.path.dirname(SCRAPER_CONFIG['output_dir']), 'all_history_final.csv')
    final_df.to_csv(final_path, index=False, date_format='%Y-%m-%d')
    logger.info(f"总数据已保存至 {final_path}")
    return final_path