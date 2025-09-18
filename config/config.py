# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'weather_db',
    'user': 'root',
    'password': '0000',  # 替换为你的数据库密码
    'port': 3306,
    'charset': 'utf8mb4',
    'local_infile': True
}

# 日志配置
LOGGING_CONFIG = {
    'log_dir': 'logs',
    'filename': 'nasa_weather_scraper.log',
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

# 抓取器配置
SCRAPER_CONFIG = {
    'api_url': 'https://power.larc.nasa.gov/api/temporal/daily/point',
    'start_year': 2020,  # 全量抓取的起始年份
    'parameters': 'T2M_MAX,T2M_MIN,T2M',  # 气象参数，可添加其他参数如RH2M,WS2M等
    'max_retries': 5,  # 最大重试次数
    'backoff_factor': 1,  # 重试退避因子
    'timeout': 30,  # 请求超时时间(秒)
    'output_dir': 'data'  # 数据输出目录
}
    