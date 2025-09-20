import logging
import os
import time
from typing import Optional
import pandas as pd
import calendar
from datetime import datetime, date, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from io import StringIO
from src.db.mysql_ops import get_db_connection, load_csv_to_db
from src.utils.common import clean_nasa_data, get_output_dir, generate_visualizations  # 新增可视化函数
from config.cities import CITIES
from config.config import SCRAPER_CONFIG

logger = logging.getLogger('nasa_weather_scraper')


def get_last_date_for_city(city_id: int) -> Optional[date]:
    """获取数据库中某城市的最新数据日期"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT MAX(date) FROM weather_daily 
                    WHERE city_id = %s
                """, (city_id,))
                result = cursor.fetchone()
                return result[0] if result[0] else None
    except Exception as e:
        logger.error(f"查询城市 {city_id} 最新数据日期失败: {e}", exc_info=True)
        return None


def calculate_date_ranges(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """计算按季度划分的日期范围列表"""
    ranges = []
    current = start_date
    while current <= end_date:
        quarter_end_month = ((current.month - 1) // 3 + 1) * 3
        quarter_end_day = calendar.monthrange(current.year, quarter_end_month)[1]
        quarter_end = date(current.year, quarter_end_month, quarter_end_day)
        segment_end = min(quarter_end, end_date)
        ranges.append((current, segment_end))
        current = segment_end + timedelta(days=1)
    return ranges


def fetch_segment_data(city_id: int, name: str, lat: float, lon: float, 
                      start_date: date, end_date: date) -> Optional[str]:
    """抓取指定时间段的数据（扩展气象指标）"""
    # 经纬度范围与格式校验
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        logger.error(f"城市 {name}（{city_id}）经纬度无效: lat={lat}, lon={lon}")
        return None
    
    # 限制经纬度精度
    lat = round(float(lat), 4)
    lon = round(float(lon), 4)

    # 格式化日期为YYYYMMDD
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    # 创建带重试机制的Session
    session = requests.Session()
    retry_strategy = Retry(
        total=SCRAPER_CONFIG['max_retries'],
        backoff_factor=SCRAPER_CONFIG['backoff_factor'],
        status_forcelist=[429, 500, 502, 503, 504, 422]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    try:
        # 扩展请求参数，增加更多气象指标
        params = {
            'parameters': 'T2M_MAX,T2M_MIN,T2M,RH2M,WS2M,PRECTOT',  # 新增多个指标
            'community': 'RE',
            'longitude': lon,
            'latitude': lat,
            'start': start_str,
            'end': end_str,
            'format': 'CSV',
        }

        if not SCRAPER_CONFIG.get('api_url'):
            logger.error("API URL未配置")
            return None

        response = session.get(
            SCRAPER_CONFIG['api_url'],
            params=params,
            timeout=SCRAPER_CONFIG['timeout']
        )
        response.raise_for_status()

        if not response.text.strip():
            logger.warning(f"城市 {name}（{city_id}）{start_str}-{end_str} 无数据返回")
            return None

        logger.debug(f"API返回原始数据:\n{response.text[:500]}")
        return response.text

    except requests.exceptions.HTTPError as e:
        response_text = response.text[:500] if 'response' in locals() else '无响应内容'
        logger.error(
            f"城市 {name}（{city_id}）HTTP错误: {e}\n"
            f"请求参数: {params}\n"
            f"响应内容: {response_text}"
        )
        
        if 'response' in locals() and response.status_code == 422:
            logger.warning("尝试单月数据抓取重试...")
            month_end = date(start_date.year, start_date.month, 
                           calendar.monthrange(start_date.year, start_date.month)[1])
            if month_end < end_date:
                return fetch_segment_data(city_id, name, lat, lon, start_date, month_end)
    except requests.exceptions.Timeout:
        logger.error(f"城市 {name}（{city_id}）请求超时")
    except Exception as e:
        logger.error(f"城市 {name}（{city_id}）{start_str}-{end_str} 抓取失败: {e}", exc_info=True)
    finally:
        session.close()
    return None


def process_city_data(city_id: int, name: str, lat: float, lon: float, 
                     end_date: date) -> Optional[pd.DataFrame]:
    """处理单个城市的数据抓取与清洗"""
    last_date = get_last_date_for_city(city_id)
    
    if last_date is None:
        start_date = date(SCRAPER_CONFIG['start_year'], 1, 1)
        logger.info(f"城市 {name}（{city_id}）无历史数据，从 {start_date} 开始全量抓取")
    else:
        start_date = last_date + timedelta(days=1)
        if start_date > end_date:
            logger.info(f"城市 {name}（{city_id}）数据已是最新，无需抓取")
            return None
        logger.info(f"城市 {name}（{city_id}）从 {start_date} 增量抓取至 {end_date}")
    
    date_ranges = calculate_date_ranges(start_date, end_date)
    city_data = []
    
    for seg_start, seg_end in date_ranges:
        logger.debug(f"城市 {name}（{city_id}）抓取 {seg_start} 至 {seg_end} 数据")
        seg_data = fetch_segment_data(city_id, name, lat, lon, seg_start, seg_end)
        
        if seg_data:
            try:
                df = clean_nasa_data(seg_data, city_id)
                if not df.empty:
                    city_data.append(df)
                    logger.debug(f"城市 {name} 抓取成功，{seg_start}至{seg_end}共{len(df)}条")
            except Exception as e:
                logger.error(f"城市 {name} 数据清洗失败: {e}", exc_info=True)
        
        time.sleep(SCRAPER_CONFIG.get('request_interval', 2))
    
    if city_data:
        # 生成该城市的可视化图表
        combined_df = pd.concat(city_data, ignore_index=True)
        generate_visualizations(combined_df, name, city_id)
        return combined_df
    return None


def fetch_incremental_data() -> Optional[str]:
    """增量抓取数据"""
    logger.info("===== 开始执行增量数据抓取流程 =====")
    
    output_dir = get_output_dir()
    os.makedirs(output_dir, exist_ok=True)
    
    all_data = []
    today = date.today()
    end_date = today - timedelta(days=1)
    if end_date < date(SCRAPER_CONFIG['start_year'], 1, 1):
        logger.warning("没有可抓取的有效日期范围")
        return None
    
    for city_id, (name, lat, lon) in CITIES.items():
        try:
            city_df = process_city_data(city_id, name, lat, lon, end_date)
            if city_df is not None:
                all_data.append(city_df)
                logger.info(f"城市 {name}（{city_id}）处理完成，共 {len(city_df)} 条")
        except Exception as e:
            logger.error(f"城市 {name}（{city_id}）处理失败: {e}", exc_info=True)
            continue
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['city_id', 'date'])
        logger.info(f"所有增量数据抓取完成，共 {len(final_df)} 条（去重后）")
        
        csv_path = os.path.join(output_dir, f"incremental_history_{today}.csv")
        final_df.to_csv(csv_path, index=False)
        logger.info(f"增量数据已保存至 {csv_path}")
        return csv_path
    else:
        logger.info("没有新的增量数据需要抓取")
        return None


def main():
    """主函数：配置日志并执行抓取流程"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/incremental_scraper.log'),
            logging.StreamHandler()
        ]
    )
    
    csv_path = fetch_incremental_data()
    
    if csv_path and os.path.exists(csv_path):
        try:
            load_csv_to_db(csv_path)
            logger.info("===== 增量数据导入完成 =====")
        except Exception as e:
            logger.error(f"增量数据导入失败: {e}", exc_info=True)


if __name__ == "__main__":
    main()