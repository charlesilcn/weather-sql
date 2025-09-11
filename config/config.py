import os
from datetime import datetime

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'weather_db',
    'user': 'root',
    'password': '0000',  # 建议实际使用时通过环境变量传入
    'port': 3306,
    'charset': 'utf8mb4',
    'local_infile': True
}

# 抓取配置
SCRAPER_CONFIG = {
    'years': range(2020, 2025),  # 抓取年份
    'output_dir': os.path.join(PROJECT_ROOT, 'data', 'nasa_weather_data'),  # 数据存储路径
    'batch_size': 1000,          # 批量写入数据库的批次大小
    'timeout': 30,               # 请求超时时间（秒）
    'retry': {
        'total': 5,              # 重试次数
        'backoff_factor': 1,     # 重试延迟因子（秒）
        'status_forcelist': [429, 500, 502, 503, 504]  # 需要重试的状态码
    }
}

# NASA API配置
NASA_API_CONFIG = {
    'url': 'https://power.larc.nasa.gov/api/temporal/daily/point',
    'parameters': 'T2M_MAX,T2M_MIN,T2M',  # 温度相关参数
    'community': 'SB',  # 社区类型（SB比RE限制宽松）
    'format': 'csv'
}

# 日志配置
LOG_CONFIG = {
    'log_dir': os.path.join(PROJECT_ROOT, 'logs'),
    'log_filename': f"nasa_scraper_{datetime.now().strftime('%Y%m%d')}.log",
    'level': 'INFO'  # 日志级别：DEBUG/INFO/WARNING/ERROR
}

# 创建必要目录（如不存在）
for dir_path in [
    SCRAPER_CONFIG['output_dir'],
    LOG_CONFIG['log_dir']
]:
    os.makedirs(dir_path, exist_ok=True)