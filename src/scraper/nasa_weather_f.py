from io import StringIO
import os
import csv
import time
import requests
import pandas as pd
import mysql.connector
from tqdm import tqdm
from datetime import datetime
from mysql.connector import Error
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from cities import CITIES  # 引用外部城市数据

# 配置参数
CONFIG = {
    # 数据库配置
    'DB': {
        'host': 'localhost',
        'database': 'weather_db',
        'user': 'root',
        'password': '0000', 
        'port': '3306'
    },
    # 抓取配置
    'SCRAPER': {
        'years': range(2020, 2025),  # 抓取2020-2024年数据
        'output_dir': 'nasa_weather_data',
        'batch_size': 1000,
        'timeout': 30,
        'retry': {
            'total': 5,
            'backoff_factor': 1,
            'status_forcelist': [429, 500, 502, 503, 504]
        }
    },
    # NASA API配置
    'NASA_API': {
        'url': 'https://power.larc.nasa.gov/api/temporal/daily/point',
        'parameters': 'T2M_MAX,T2M_MIN,T2M,RH2M,WS10M,PS',  # 气象参数
        'community': 'RE',
        'format': 'csv'
    }
}

# 处理城市数据格式（适配cities.py中的数据结构）
def get_formatted_cities():
    """将cities.py中的城市数据格式转换为包含省份的结构"""
    # 完整省份映射表（覆盖cities.py中所有294个城市ID）
    province_mapping = {
        # 北京市
        1: "北京",
        # 天津市
        2: "天津",
        # 河北省
        3: "河北",  # 石家庄
        # 山西省
        4: "山西",  # 太原
        # 内蒙古自治区
        5: "内蒙古",  # 呼和浩特
        # 辽宁省
        6: "辽宁",  # 沈阳
        7: "辽宁",  # 大连
        # 吉林省
        8: "吉林",  # 长春
        # 黑龙江省
        9: "黑龙江",  # 哈尔滨
        # 上海市
        10: "上海",
        # 江苏省
        11: "江苏",  # 南京
        12: "江苏",  # 苏州
        13: "江苏",  # 无锡
        14: "江苏",  # 常州
        15: "江苏",  # 镇江
        16: "江苏",  # 扬州
        17: "江苏",  # 泰州
        # 浙江省
        18: "浙江",  # 杭州
        19: "浙江",  # 宁波
        20: "浙江",  # 温州
        21: "浙江",  # 嘉兴
        22: "浙江",  # 湖州
        23: "浙江",  # 绍兴
        24: "浙江",  # 金华
        25: "浙江",  # 衢州
        26: "浙江",  # 舟山
        27: "浙江",  # 台州
        28: "浙江",  # 丽水
        # 安徽省
        29: "安徽",  # 合肥
        30: "安徽",  # 芜湖
        31: "安徽",  # 蚌埠
        32: "安徽",  # 淮南
        33: "安徽",  # 马鞍山
        34: "安徽",  # 淮北
        35: "安徽",  # 铜陵
        36: "安徽",  # 安庆
        37: "安徽",  # 黄山
        38: "安徽",  # 滁州
        39: "安徽",  # 阜阳
        40: "安徽",  # 宿州
        41: "安徽",  # 六安
        42: "安徽",  # 亳州
        43: "安徽",  # 池州
        44: "安徽",  # 宣城
        # 福建省
        45: "福建",  # 福州
        46: "福建",  # 厦门
        47: "福建",  # 莆田
        48: "福建",  # 三明
        49: "福建",  # 泉州
        50: "福建",  # 漳州
        51: "福建",  # 南平
        52: "福建",  # 龙岩
        53: "福建",  # 宁德
        # 江西省
        54: "江西",  # 南昌
        55: "江西",  # 景德镇
        56: "江西",  # 萍乡
        57: "江西",  # 九江
        58: "江西",  # 新余
        59: "江西",  # 鹰潭
        60: "江西",  # 赣州
        61: "江西",  # 吉安
        62: "江西",  # 宜春
        63: "江西",  # 抚州
        64: "江西",  # 上饶
        # 山东省
        65: "山东",  # 济南
        66: "山东",  # 青岛
        67: "山东",  # 淄博
        68: "山东",  # 枣庄
        69: "山东",  # 东营
        70: "山东",  # 烟台
        71: "山东",  # 潍坊
        72: "山东",  # 济宁
        73: "山东",  # 泰安
        74: "山东",  # 威海
        75: "山东",  # 日照
        76: "山东",  # 临沂
        77: "山东",  # 德州
        78: "山东",  # 聊城
        79: "山东",  # 滨州
        80: "山东",  # 菏泽
        # 河南省
        81: "河南",  # 郑州
        82: "河南",  # 开封
        83: "河南",  # 洛阳
        84: "河南",  # 平顶山
        85: "河南",  # 安阳
        86: "河南",  # 鹤壁
        87: "河南",  # 新乡
        88: "河南",  # 焦作
        89: "河南",  # 濮阳
        90: "河南",  # 许昌
        91: "河南",  # 漯河
        92: "河南",  # 三门峡
        93: "河南",  # 南阳
        94: "河南",  # 商丘
        95: "河南",  # 信阳
        96: "河南",  # 周口
        97: "河南",  # 驻马店
        # 湖北省
        98: "湖北",  # 武汉
        99: "湖北",  # 黄石
        100: "湖北",  # 十堰
        101: "湖北",  # 宜昌
        102: "湖北",  # 襄阳
        103: "湖北",  # 鄂州
        104: "湖北",  # 荆门
        105: "湖北",  # 孝感
        106: "湖北",  # 荆州
        107: "湖北",  # 黄冈
        108: "湖北",  # 咸宁
        109: "湖北",  # 随州
        110: "湖北",  # 恩施
        # 湖南省
        111: "湖南",  # 长沙
        112: "湖南",  # 株洲
        113: "湖南",  # 湘潭
        114: "湖南",  # 衡阳
        115: "湖南",  # 邵阳
        116: "湖南",  # 岳阳
        117: "湖南",  # 常德
        118: "湖南",  # 张家界
        119: "湖南",  # 益阳
        120: "湖南",  # 郴州
        121: "湖南",  # 永州
        122: "湖南",  # 怀化
        123: "湖南",  # 娄底
        124: "湖南",  # 湘西
        # 广东省
        125: "广东",  # 广州
        126: "广东",  # 韶关
        127: "广东",  # 深圳
        128: "广东",  # 珠海
        129: "广东",  # 汕头
        130: "广东",  # 佛山
        131: "广东",  # 江门
        132: "广东",  # 湛江
        133: "广东",  # 茂名
        134: "广东",  # 肇庆
        135: "广东",  # 惠州
        136: "广东",  # 梅州
        137: "广东",  # 汕尾
        138: "广东",  # 河源
        139: "广东",  # 阳江
        140: "广东",  # 清远
        141: "广东",  # 东莞
        142: "广东",  # 中山
        143: "广东",  # 潮州
        144: "广东",  # 揭阳
        145: "广东",  # 云浮
        # 广西壮族自治区
        146: "广西",  # 南宁
        147: "广西",  # 柳州
        148: "广西",  # 桂林
        149: "广西",  # 梧州
        150: "广西",  # 北海
        151: "广西",  # 防城港
        152: "广西",  # 钦州
        153: "广西",  # 贵港
        154: "广西",  # 玉林
        155: "广西",  # 百色
        156: "广西",  # 贺州
        157: "广西",  # 河池
        158: "广西",  # 来宾
        159: "广西",  # 崇左
        # 海南省
        160: "海南",  # 海口
        161: "海南",  # 三亚
        162: "海南",  # 三沙
        163: "海南",  # 儋州
        164: "海南",  # 五指山
        165: "海南",  # 琼海
        166: "海南",  # 文昌
        167: "海南",  # 万宁
        168: "海南",  # 东方
        169: "海南",  # 定安
        170: "海南",  # 屯昌
        171: "海南",  # 澄迈
        172: "海南",  # 临高
        173: "海南",  # 白沙
        174: "海南",  # 昌江
        175: "海南",  # 乐东
        176: "海南",  # 陵水
        177: "海南",  # 保亭
        178: "海南",  # 琼中
        # 重庆市
        179: "重庆",
        # 四川省
        180: "四川",  # 成都
        181: "四川",  # 自贡
        182: "四川",  # 攀枝花
        183: "四川",  # 泸州
        184: "四川",  # 德阳
        185: "四川",  # 绵阳
        186: "四川",  # 广元
        187: "四川",  # 遂宁
        188: "四川",  # 内江
        189: "四川",  # 乐山
        190: "四川",  # 南充
        191: "四川",  # 眉山
        192: "四川",  # 宜宾
        193: "四川",  # 广安
        194: "四川",  # 达州
        195: "四川",  # 雅安
        196: "四川",  # 巴中
        197: "四川",  # 资阳
        198: "四川",  # 阿坝
        199: "四川",  # 甘孜
        200: "四川",  # 凉山
        # 贵州省
        201: "贵州",  # 贵阳
        202: "贵州",  # 六盘水
        203: "贵州",  # 遵义
        204: "贵州",  # 安顺
        205: "贵州",  # 毕节
        206: "贵州",  # 铜仁
        207: "贵州",  # 黔西南
        208: "贵州",  # 黔东南
        209: "贵州",  # 黔南
        # 云南省
        210: "云南",  # 昆明
        211: "云南",  # 曲靖
        212: "云南",  # 玉溪
        213: "云南",  # 保山
        214: "云南",  # 昭通
        215: "云南",  # 丽江
        216: "云南",  # 普洱
        217: "云南",  # 临沧
        218: "云南",  # 楚雄
        219: "云南",  # 红河
        220: "云南",  # 文山
        221: "云南",  # 西双版纳
        222: "云南",  # 大理
        223: "云南",  # 德宏
        224: "云南",  # 怒江
        225: "云南",  # 迪庆
        # 西藏自治区
        226: "西藏",  # 拉萨
        227: "西藏",  # 日喀则
        228: "西藏",  # 昌都
        229: "西藏",  # 林芝
        230: "西藏",  # 山南
        231: "西藏",  # 那曲
        232: "西藏",  # 阿里
        # 陕西省
        233: "陕西",  # 西安
        234: "陕西",  # 铜川
        235: "陕西",  # 宝鸡
        236: "陕西",  # 咸阳
        237: "陕西",  # 渭南
        238: "陕西",  # 延安
        239: "陕西",  # 汉中
        240: "陕西",  # 榆林
        241: "陕西",  # 安康
        242: "陕西",  # 商洛
        # 甘肃省
        243: "甘肃",  # 兰州
        244: "甘肃",  # 嘉峪关
        245: "甘肃",  # 金昌
        246: "甘肃",  # 白银
        247: "甘肃",  # 天水
        248: "甘肃",  # 武威
        249: "甘肃",  # 张掖
        250: "甘肃",  # 平凉
        251: "甘肃",  # 酒泉
        252: "甘肃",  # 庆阳
        253: "甘肃",  # 定西
        254: "甘肃",  # 陇南
        255: "甘肃",  # 临夏
        256: "甘肃",  # 甘南
        # 青海省
        257: "青海",  # 西宁
        258: "青海",  # 海东
        259: "青海",  # 海北
        260: "青海",  # 黄南
        261: "青海",  # 海南
        262: "青海",  # 果洛
        263: "青海",  # 玉树
        264: "青海",  # 海西
        # 宁夏回族自治区
        265: "宁夏",  # 银川
        266: "宁夏",  # 石嘴山
        267: "宁夏",  # 吴忠
        268: "宁夏",  # 固原
        269: "宁夏",  # 中卫
        # 新疆维吾尔自治区
        270: "新疆",  # 乌鲁木齐
        271: "新疆",  # 克拉玛依
        272: "新疆",  # 吐鲁番
        273: "新疆",  # 哈密
        274: "新疆",  # 昌吉
        275: "新疆",  # 博尔塔拉
        276: "新疆",  # 巴音郭楞
        277: "新疆",  # 阿克苏
        278: "新疆",  # 克孜勒苏
        279: "新疆",  # 喀什
        280: "新疆",  # 和田
        281: "新疆",  # 伊犁
        282: "新疆",  # 塔城
        283: "新疆",  # 阿勒泰
        284: "新疆",  # 石河子
        285: "新疆",  # 阿拉尔
        286: "新疆",  # 图木舒克
        287: "新疆",  # 五家渠
        288: "新疆",  # 北屯
        289: "新疆",  # 铁门关
        290: "新疆",  # 双河
        291: "新疆",  # 可克达拉
        292: "新疆",  # 昆玉
        # 香港特别行政区
        293: "香港",
        # 澳门特别行政区
        294: "澳门"
    }
    
    formatted = {}
    for city_id, (name, lat, lon) in CITIES.items():
        province = province_mapping.get(city_id, "未知")
        formatted[city_id] = (name, province, lat, lon)
    return formatted

# 更新为格式化后的城市数据
CITIES = get_formatted_cities()

def create_database_connection():
    """创建数据库连接"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=CONFIG['DB']['host'],
            database=CONFIG['DB']['database'],
            user=CONFIG['DB']['user'],
            password=CONFIG['DB']['password'],
            port=CONFIG['DB']['port']
        )
        if connection.is_connected():
            print(f"成功连接到MySQL服务器版本: {connection.server_info}")
            return connection
    except Error as e:
        print(f"数据库连接错误: {e}")
    return connection

def create_tables(connection):
    """创建城市表和气象数据表"""
    cursor = connection.cursor()
    
    # 创建城市表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cities (
        city_id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        province VARCHAR(100) NOT NULL,
        latitude DECIMAL(10, 6) NOT NULL,
        longitude DECIMAL(10, 6) NOT NULL,
        UNIQUE KEY unique_location (latitude, longitude)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''')
    
    # 创建气象数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        city_id INT NOT NULL,
        date DATE NOT NULL,
        temp_max_c DECIMAL(6, 2),
        temp_min_c DECIMAL(6, 2),
        temp_avg_c DECIMAL(6, 2),
        humidity_avg DECIMAL(6, 2),
        wind_speed_kmh DECIMAL(6, 2),
        pressure_hpa DECIMAL(8, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (city_id) REFERENCES cities(city_id),
        UNIQUE KEY unique_city_date (city_id, date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''')
    
    connection.commit()
    print("数据库表检查/创建完成")

def init_cities_table(connection):
    """初始化城市表数据"""
    cursor = connection.cursor()
    insert_count = 0
    
    for city_id, (name, province, lat, lon) in CITIES.items():
        try:
            cursor.execute('''
            INSERT IGNORE INTO cities 
            (city_id, name, province, latitude, longitude) 
            VALUES (%s, %s, %s, %s, %s)
            ''', (city_id, name, province, lat, lon))
            insert_count += cursor.rowcount
        except Error as e:
            print(f"插入城市数据错误 (ID: {city_id}): {e}")
    
    connection.commit()
    print(f"城市表初始化完成，新增 {insert_count} 个城市数据")

def create_request_session():
    """创建带有重试机制的请求会话"""
    session = requests.Session()
    retry = Retry(
        total=CONFIG['SCRAPER']['retry']['total'],
        backoff_factor=CONFIG['SCRAPER']['retry']['backoff_factor'],
        status_forcelist=CONFIG['SCRAPER']['retry']['status_forcelist']
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_weather_data(session, city_id, year, lat, lon):
    """从NASA API抓取单个城市单一年份的气象数据"""
    params = {
        'parameters': CONFIG['NASA_API']['parameters'],
        'community': CONFIG['NASA_API']['community'],
        'latitude': lat,
        'longitude': lon,
        'start': f"{year}0101",
        'end': f"{year}1231",
        'format': CONFIG['NASA_API']['format']
    }
    
    try:
        response = session.get(
            CONFIG['NASA_API']['url'],
            params=params,
            timeout=CONFIG['SCRAPER']['timeout']
        )
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"API请求失败 (城市ID: {city_id}, 年份: {year}, 状态码: {response.status_code})")
            return None
    except Exception as e:
        print(f"抓取数据错误 (城市ID: {city_id}, 年份: {year}): {str(e)}")
        return None

def parse_weather_data(csv_data, city_id, year):
    """解析CSV格式的气象数据为DataFrame"""
    if not csv_data:
        return None
    
    lines = csv_data.splitlines()
    data_start = None
    for i, line in enumerate(lines):
        if line.startswith('YEAR,MO,DY'):
            data_start = i
            break
    
    if data_start is None:
        return None
    
    try:
        df = pd.read_csv(
    StringIO('\n'.join(lines[data_start:]))
        )
        df['date'] = pd.to_datetime(df[['YEAR', 'MO', 'DY']].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d')
        
        df = df.rename(columns={
            'T2M_MAX': 'temp_max_c',
            'T2M_MIN': 'temp_min_c',
            'T2M': 'temp_avg_c',
            'RH2M': 'humidity_avg',
            'WS10M': 'wind_speed_kmh',
            'PS': 'pressure_hpa'
        })
        
        df['city_id'] = city_id
        
        # 过滤无效值 (-999是NASA的缺测标记)
        for col in ['temp_max_c', 'temp_min_c', 'temp_avg_c', 
                   'humidity_avg', 'wind_speed_kmh', 'pressure_hpa']:
            if col in df.columns:
                df = df[df[col] != -999]
        
        return df[['city_id', 'date', 'temp_max_c', 'temp_min_c', 
                  'temp_avg_c', 'humidity_avg', 'wind_speed_kmh', 'pressure_hpa']]
    
    except Exception as e:
        print(f"解析数据错误 (城市ID: {city_id}, 年份: {year}): {str(e)}")
        return None

def batch_insert_data(connection, data_list):
    """批量插入数据到数据库"""
    if not data_list:
        return 0
    
    cursor = connection.cursor()
    insert_query = '''
    INSERT IGNORE INTO weather_data 
    (city_id, date, temp_max_c, temp_min_c, temp_avg_c, 
     humidity_avg, wind_speed_kmh, pressure_hpa) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''
    
    try:
        cursor.executemany(insert_query, data_list)
        connection.commit()
        return cursor.rowcount
    except Error as e:
        print(f"批量插入错误: {e}")
        connection.rollback()
        return 0

def process_city_year(session, connection, city_id, city_info, year, output_dir):
    """处理单个城市单一年份的数据: 抓取、解析、保存、入库"""
    name, province, lat, lon = city_info
    
    # 创建保存目录
    city_dir = os.path.join(output_dir, f"city_{city_id}")
    os.makedirs(city_dir, exist_ok=True)
    
    # 检查本地缓存
    filename = os.path.join(city_dir, f"{year}.csv")
    csv_data = None
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                csv_data = f.read()
            print(f"使用本地缓存 (城市: {name}, 年份: {year})")
        except Exception as e:
            print(f"读取本地文件失败，重新抓取: {e}")
    
    # 缓存不存在则从API抓取
    if not csv_data:
        csv_data = fetch_weather_data(session, city_id, year, lat, lon)
        if csv_data:
            try:
                with open(filename, 'w') as f:
                    f.write(csv_data)
            except Exception as e:
                print(f"保存文件失败: {e}")
    
    # 解析并入库
    df = parse_weather_data(csv_data, city_id, year)
    if df is None or df.empty:
        return 0
    
    data_list = [
        (row['city_id'], row['date'].strftime('%Y-%m-%d'),
         row['temp_max_c'], row['temp_min_c'], row['temp_avg_c'],
         row['humidity_avg'], row['wind_speed_kmh'], row['pressure_hpa'])
        for _, row in df.iterrows()
    ]
    
    return batch_insert_data(connection, data_list)

def main():
    """主函数"""
    start_time = time.time()
    total_cities = len(CITIES)
    total_years = len(CONFIG['SCRAPER']['years'])
    total_records = 0
    
    print(f"开始处理 {total_cities} 个城市 {CONFIG['SCRAPER']['years'][0]} 至 {CONFIG['SCRAPER']['years'][-1]} 年的气象数据")
    
    # 创建输出目录
    output_dir = CONFIG['SCRAPER']['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    
    # 初始化数据库
    connection = create_database_connection()
    if not connection:
        print("数据库连接失败，程序退出")
        return
    
    create_tables(connection)
    init_cities_table(connection)
    
    # 创建请求会话
    session = create_request_session()
    
    try:
        for year in CONFIG['SCRAPER']['years']:
            year_start = time.time()
            year_records = 0
            print(f"\n===== 处理 {year} 年数据 =====")
            
            for city_id, city_info in tqdm(CITIES.items(), total=total_cities, 
                                          desc=f"{year}年处理进度"):
                records = process_city_year(session, connection, city_id, city_info, year, output_dir)
                year_records += records
                total_records += records
                
                # 每处理10个城市休眠1秒，避免请求过于频繁
                if city_id % 10 == 0:
                    time.sleep(1)
            
            print(f"{year}年处理完成，新增记录: {year_records} 条，耗时: {time.time()-year_start:.2f}秒")
        
        # 输出统计信息
        elapsed = time.time() - start_time
        print(f"\n===== 所有处理完成 =====")
        print(f"总城市数: {total_cities}, 总年份: {total_years}")
        print(f"总新增记录: {total_records}, 总耗时: {elapsed:.2f}秒")
        
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"运行错误: {str(e)}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main()