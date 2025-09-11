from src.scraper.nasa_scraper import fetch_all_cities
from src.db.mysql_ops import load_csv_to_db
from src.utils.common import logger

def main():
    logger.info("===== 开始执行NASA天气数据抓取流程 =====")
    try:
        # 1. 抓取所有数据并保存为CSV
        csv_path = fetch_all_cities()
        if not csv_path:
            logger.warning("流程终止：未生成有效数据文件")
            return
        # 2. 将CSV数据写入数据库
        load_csv_to_db(csv_path)
        logger.info("===== 所有流程执行完成 =====")
    except Exception as e:
        logger.error(f"流程执行失败: {e}", exc_info=True)

if __name__ == "__main__":
    main()