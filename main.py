import logging
import os
from src.scraper.nasa_scraper import fetch_incremental_data  # 导入增量抓取函数
from src.db.mysql_ops import load_csv_to_db
from config.config import LOGGING_CONFIG

def setup_logging():
    """配置日志系统"""
    log_dir = LOGGING_CONFIG['log_dir']
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=LOGGING_CONFIG['level'],
        format=LOGGING_CONFIG['format'],
        handlers=[
            logging.FileHandler(os.path.join(log_dir, LOGGING_CONFIG['filename'])),
            logging.StreamHandler()
        ]
    )

def main(mode='full'):
    """主函数：支持全量抓取和增量抓取两种模式"""
    try:
        if mode == 'full':
            logger.info("===== 开始执行全量数据抓取流程 =====")
            csv_path = fetch_all_cities()
        elif mode == 'incremental':
            logger.info("===== 开始执行增量数据抓取流程 =====")
            csv_path = fetch_incremental_data()
        else:
            logger.error(f"未知模式: {mode}")
            return
        
        if csv_path and os.path.exists(csv_path):
            logger.info(f"开始将数据导入数据库: {csv_path}")
            load_csv_to_db(csv_path)
            logger.info("===== 所有流程执行完成 =====")
        else:
            logger.info("没有数据需要导入数据库")
            
    except Exception as e:
        logger.error(f"流程执行失败: {e}", exc_info=True)

if __name__ == "__main__":
    import argparse
    
    # 设置日志
    setup_logging()
    logger = logging.getLogger('nasa_weather_scraper')
    
    # 解析命令行参数，支持选择全量或增量模式
    parser = argparse.ArgumentParser(description='NASA气象数据抓取工具')
    parser.add_argument('--mode', 
                      choices=['full', 'incremental'], 
                      default='full',
                      help='抓取模式: full(全量) 或 incremental(增量)')
    args = parser.parse_args()
    
    # 执行主函数
    main(args.mode)
    