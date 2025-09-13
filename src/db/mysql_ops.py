import pymysql
from config.config import DB_CONFIG
from src.utils.common import logger

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        logger.info("数据库连接成功")
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        raise

def load_csv_to_db(csv_path):
    """将CSV文件批量写入数据库"""
    if not csv_path:
        logger.warning("无CSV文件路径，跳过数据库写入")
        return
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 禁用外键检查（加速写入）
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            # 清空表（如需增量写入，可删除此行并修改SQL）
            cursor.execute("TRUNCATE TABLE weather_daily")
            
            # 构建LOAD DATA SQL（适配Windows路径）
            csv_unix_path = csv_path.replace('\\', '/')
            sql = """
            LOAD DATA LOCAL INFILE %s
            INTO TABLE weather_daily
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (city_id, @date_str, temp_max_c, temp_min_c, temp_avg_c)
            SET date = STR_TO_DATE(@date_str, '%%Y-%%m-%%d')
            """
            cursor.execute(sql, (csv_unix_path,))
            conn.commit()
            
            # 统计写入行数
            cursor.execute("SELECT COUNT(*) FROM weather_daily")
            count = cursor.fetchone()[0]
            logger.info(f"数据写入完成，共 {count} 条记录")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"数据写入失败: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭")