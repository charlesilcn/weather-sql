from src.db.mysql_ops import get_db_connection

def query_city_temp(city_id, start_date, end_date):
    """查询某城市某时间段的温度数据"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
            SELECT date, temp_max_c, temp_min_c, temp_avg_c
            FROM weather_daily
            WHERE city_id = %s AND date BETWEEN %s AND %s
            ORDER BY date
            """
            cursor.execute(sql, (city_id, start_date, end_date))
            return cursor.fetchall()
    finally:
        if conn:
            conn.close()

# 示例：查询北京（city_id=1）2024年1月数据
if __name__ == "__main__":
    data = query_city_temp(1, "2024-01-01", "2024-01-31")
    print("北京2024年1月温度数据：")
    for row in data[:5]:  # 打印前5条
        print(f"日期：{row[0]}, 最高温：{row[1]}℃, 最低温：{row[2]}℃")