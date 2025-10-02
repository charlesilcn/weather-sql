import re
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
from io import StringIO
from datetime import datetime
import pandas as pd
import logging
import os
from config.config import SCRAPER_CONFIG
import numpy as np
from scipy import stats

warnings.filterwarnings("ignore", category=UserWarning, message="Glyph .* missing from font.*")

# 强制使用Arial字体（适配英文标签）
plt.rcParams["font.family"] = ["Arial"]
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示异常
sns.set(font_scale=1.2)  # 调整图表字体大小
sns.set_style("whitegrid")  # 图表网格样式

# 初始化日志（确保后续函数可调用）
logger = logging.getLogger('nasa_weather_scraper')

# -------------------------- 2. 目录工具函数（功能不变） --------------------------
def get_output_dir() -> str:
    """Get data output directory, create if not exists"""
    output_dir = SCRAPER_CONFIG['output_dir']
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def get_visualization_dir() -> str:
    """Get visualization output directory, create if not exists"""
    viz_dir = os.path.join(get_output_dir(), 'visualizations')
    if not os.path.exists(viz_dir):
        os.makedirs(viz_dir)
    return viz_dir

# -------------------------- 3. NASA数据清洗核心函数（功能不变，日志英文适配） --------------------------
def clean_nasa_data(csv_str: str, city_id: int) -> pd.DataFrame:
    """
    Clean NASA CSV data: extract valid rows, unify column names, filter invalid values
    Returns: Cleaned DataFrame (empty if cleaning fails)
    """
    lines = csv_str.splitlines()
    if len(lines) > 20:
        logger.debug(f"City ID {city_id} raw data (first 20 lines):\n" + "\n".join(lines[:20]))

    # Locate data start row (skip comment lines)
    data_start_row = 5  # Default: skip first 5 comment lines (NASA common format)
    for i, line in enumerate(lines):
        lower_line = line.lower()
        if ('year' in lower_line or 'mo' in lower_line or 'dy' in lower_line) and ('t2m' in lower_line):
            data_start_row = i
            break

    # Parse CSV with pandas
    try:
        df = pd.read_csv(
            StringIO(csv_str),
            skiprows=data_start_row,
            sep=',',
            on_bad_lines='skip',
            engine='python'
        )
    except Exception as e:
        logger.error(f"City ID {city_id} CSV parsing failed: {e}")
        return pd.DataFrame()

    # Unify column names (match NASA standard parameter names)
    column_mapping = {
        # Date-related columns (required for date merging)
        'year': 'year', 'yr': 'year', 'y': 'year',
        'mo': 'month', 'month': 'month', 'm': 'month',
        'dy': 'day', 'day': 'day', 'd': 'day',
        # Weather metrics (match NASA API standard names)
        't2m_max': 't2m_max', 't2mmax': 't2m_max',
        't2m_min': 't2m_min', 't2mmin': 't2m_min',
        't2m': 't2m', 'temperature': 't2m',
        'rh2m': 'rh2m', 'relative_humidity': 'rh2m',
        'ws2m': 'ws2m', 'wind_speed': 'ws2m',
        'prectot': 'precip', 'precipitation': 'precip'
    }
    # Only keep valid columns (avoid renaming errors)
    valid_mapping = {}
    for raw_col in df.columns:
        lower_raw = raw_col.lower()
        for key, target in column_mapping.items():
            if key == lower_raw:
                valid_mapping[raw_col] = target
                break
    df = df.rename(columns=valid_mapping)

    # Check required date columns (data is invalid without these)
    required_date_cols = ['year', 'month', 'day']
    missing_date = [col for col in required_date_cols if col not in df.columns]
    if missing_date:
        logger.error(f"City ID {city_id} missing date columns: {missing_date}")
        return pd.DataFrame()

    # Add city ID + clean invalid data
    df['city_id'] = city_id
    try:
        # Merge year/month/day into 'date' column
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
        # Filter: empty date, NASA missing values (-999/-9999)
        df = df.dropna(subset=['date'])
        for col in ['t2m_max', 't2m_min', 't2m', 'rh2m', 'ws2m', 'precip']:
            if col in df.columns:
                df = df[~df[col].isin([-999, -9999, 'NaN', 'NA'])]
    except Exception as e:
        logger.error(f"City ID {city_id} data filtering failed: {e}")
        return pd.DataFrame()

    # Keep only necessary columns
    final_cols = ['city_id', 'date'] + [col for col in ['t2m_max', 't2m_min', 't2m', 'rh2m', 'ws2m', 'precip'] if col in df.columns]
    return df[final_cols]

# -------------------------- 4. 可视化生成函数（全英文标签，适配Arial） --------------------------
def generate_visualizations(df: pd.DataFrame, city_name: str, city_id: int):
    """
    Generate 3 types of charts for single city (all English labels for Arial font):
    1. Temperature Trend (Max/Min/Avg)
    2. Wind Speed vs Relative Humidity
    3. Monthly Precipitation Distribution
    Charts are saved to 'visualizations' directory automatically
    """
    if df.empty:
        logger.warning(f"City {city_name} ({city_id}) has no valid data, skip visualization")
        return

    viz_dir = get_visualization_dir()
    today = datetime.now().strftime("%Y%m%d")  # Add date to filename (avoid overwriting)
    plt.close('all')  # Close old plots to save memory

    # 1. Temperature Trend Chart
    if all(col in df.columns for col in ['t2m_max', 't2m_min', 't2m']):
        plt.figure(figsize=(12, 6))
        # Plot with English labels and distinct colors
        sns.lineplot(data=df, x='date', y='t2m_max', label='Max Temperature (°C)', color='#ff6b6b')
        sns.lineplot(data=df, x='date', y='t2m_min', label='Min Temperature (°C)', color='#4ecdc4')
        sns.lineplot(data=df, x='date', y='t2m', label='Average Temperature (°C)', color='#45b7d1')
        # English title and labels
        plt.title(f'{city_name} Temperature Trend', fontsize=14)
        plt.xlabel('Date')
        plt.ylabel('Temperature (°C)')
        plt.xticks(rotation=45)  # Rotate date labels for readability
        plt.legend(loc='best')
        plt.tight_layout()  # Auto-adjust layout to avoid label cutoff
        # Save plot
        temp_path = os.path.join(viz_dir, f'{city_id}_{city_name}_temperature_{today}.png')
        plt.savefig(temp_path, dpi=100)  # dpi=100 for clear image
        logger.info(f"Temperature trend chart saved to: {temp_path}")

    # 2. Wind Speed vs Relative Humidity Scatter Chart
    if all(col in df.columns for col in ['ws2m', 'rh2m']):
        df['month'] = df['date'].dt.month  # Color by month
        plt.figure(figsize=(10, 5))
        sns.scatterplot(
            data=df, 
            x='ws2m', 
            y='rh2m', 
            hue='month', 
            palette='viridis',  # Colorful palette for month distinction
            alpha=0.7  # Transparency to avoid overlap
        )
        # English title and labels
        plt.title(f'{city_name} Avg Wind Speed vs Relative Humidity', fontsize=14)
        plt.xlabel('Average Wind Speed (m/s)')
        plt.ylabel('Average Relative Humidity (%)')
        plt.legend(title='Month', bbox_to_anchor=(1.05, 1), loc='upper left')  # Move legend to avoid overlap
        plt.tight_layout()
        # Save plot
        ws_rh_path = os.path.join(viz_dir, f'{city_id}_{city_name}_wind_humidity_{today}.png')
        plt.savefig(ws_rh_path, dpi=100)
        logger.info(f"Wind-Humidity chart saved to: {ws_rh_path}")

    # 3. Monthly Precipitation Distribution Chart
    if 'precip' in df.columns:
        df['month'] = df['date'].dt.month
        plt.figure(figsize=(10, 5))
        sns.boxplot(
            data=df, 
            x='month', 
            y='precip', 
            palette='Set2'  # Soft palette for boxplot
        )
        # English title and labels (month as number, no Chinese)
        plt.title(f'{city_name} Monthly Precipitation Distribution', fontsize=14)
        plt.xlabel('Month')
        plt.ylabel('Precipitation (mm)')
        plt.xticks(range(0, 12), [f'{i+1}' for i in range(12)])  # Show month as 1-12
        plt.tight_layout()
        # Save plot
        precip_path = os.path.join(viz_dir, f'{city_id}_{city_name}_precipitation_{today}.png')
        plt.savefig(precip_path, dpi=100)
        logger.info(f"Monthly precipitation chart saved to: {precip_path}")

def analyze_weather_data(df: pd.DataFrame, city_name: str, city_id: int) -> tuple[pd.DataFrame, dict]:
    """
    深度分析天气数据：计算关键指标+统计信息
    Args:
        df: 清洗后的城市天气DataFrame
        city_name: 城市名
        city_id: 城市ID
    Returns:
        analysis_df: 月度统计结果DataFrame
        stats_dict: 关键统计信息（极端值、相关性等）
    """
    # 1. 数据预处理：添加年月字段（用于分组统计）
    df_analysis = df.copy()
    df_analysis['year_month'] = df_analysis['date'].dt.strftime("%Y-%m")  # 格式：2025-09
    df_analysis['month'] = df_analysis['date'].dt.month
    df_analysis['year'] = df_analysis['date'].dt.year

    # 2. 月度统计（核心指标均值/最大值/最小值）
    monthly_stats = df_analysis.groupby('year_month').agg({
        't2m_max': ['mean', 'max', 'min'],  # 最高温：月均值、月极值
        't2m_min': ['mean', 'max', 'min'],  # 最低温：月均值、月极值
        't2m': ['mean', 'std'],             # 平均温：月均值、标准差（波动程度）
        'ws2m': ['mean', 'max'],            # 风速：月均值、月最大值
        'rh2m': ['mean', 'std'],            # 湿度：月均值、标准差
        'precip': ['sum', 'mean']           # 降水量：月总量、月均值
    }).round(2)  # 保留2位小数

    # 重命名统计列（避免多层列名）
    monthly_stats.columns = [
        't2m_max_mean', 't2m_max_max', 't2m_max_min',
        't2m_min_mean', 't2m_min_max', 't2m_min_min',
        't2m_mean', 't2m_std',
        'ws2m_mean', 'ws2m_max',
        'rh2m_mean', 'rh2m_std',
        'precip_sum', 'precip_mean'
    ]
    monthly_stats.reset_index(inplace=True)  # 重置索引，保留year_month字段
    monthly_stats['city_id'] = city_id
    monthly_stats['city_name'] = city_name

    # 3. 关键统计信息（极端值、相关性等）
    stats_dict = {
        'city_name': city_name,
        'city_id': city_id,
        'date_range': f"{df_analysis['date'].min().strftime('%Y-%m-%d')} ~ {df_analysis['date'].max().strftime('%Y-%m-%d')}",
        'total_days': len(df_analysis),  # 有效数据天数
        # 极端温度
        'extreme_high_temp': df_analysis['t2m_max'].max(),
        'extreme_high_date': df_analysis[df_analysis['t2m_max'] == df_analysis['t2m_max'].max()]['date'].iloc[0].strftime('%Y-%m-%d'),
        'extreme_low_temp': df_analysis['t2m_min'].min(),
        'extreme_low_date': df_analysis[df_analysis['t2m_min'] == df_analysis['t2m_min'].min()]['date'].iloc[0].strftime('%Y-%m-%d'),
        # 温湿度相关性（Pearson相关系数：-1负相关，1正相关，0无相关）
        'temp_humidity_corr': round(stats.pearsonr(df_analysis['t2m'], df_analysis['rh2m'])[0], 3) if 'rh2m' in df_analysis.columns else None,
        # 月均温最高/最低的月份
        'hottest_month': monthly_stats.loc[monthly_stats['t2m_mean'].idxmax(), 'year_month'],
        'hottest_month_temp': monthly_stats['t2m_mean'].max(),
        'coldest_month': monthly_stats.loc[monthly_stats['t2m_mean'].idxmin(), 'year_month'],
        'coldest_month_temp': monthly_stats['t2m_mean'].min()
    }

    return monthly_stats, stats_dict

def generate_advanced_visualizations(df: pd.DataFrame, analysis_df: pd.DataFrame, stats_dict: dict):
    """
    生成高级可视化图表：热力图、箱线图、统计表格等
    """
    city_name = stats_dict['city_name']
    city_id = stats_dict['city_id']
    viz_dir = get_visualization_dir()
    today = datetime.now().strftime("%Y%m%d")
    plt.close('all')

    # -------------------------- 1. 月度温度热力图（按年月展示温度分布） --------------------------
    if not analysis_df.empty:
        # 准备热力图数据（行：年份，列：月份，值：月均温）
        analysis_df['year'] = analysis_df['year_month'].str.split('-').str[0]
        analysis_df['month_num'] = analysis_df['year_month'].str.split('-').str[1].astype(int)
        heatmap_data = analysis_df.pivot(index='year', columns='month_num', values='t2m_mean')
        heatmap_data.columns = [f"{i}月" for i in range(1, 13)]  # 列名改为“1月”“2月”...

        plt.figure(figsize=(14, 8))
        sns.heatmap(
            heatmap_data,
            annot=True,  # 显示数值
            fmt='.1f',   # 数值格式（1位小数）
            cmap='YlOrRd',  # 热力图颜色（红黄色系，高温红色，低温黄色）
            cbar_kws={'label': 'Average Temperature (°C)'},  # 颜色条标签
            missing_values=np.nan,
            annot_kws={'fontsize': 9}  # 数值字体大小
        )
        plt.title(f'{city_name} Monthly Average Temperature Heatmap\n{stats_dict["date_range"]}', fontsize=14)
        plt.xlabel('Month')
        plt.ylabel('Year')
        plt.tight_layout()
        heatmap_path = os.path.join(viz_dir, f'{city_id}_{city_name}_temp_heatmap_{today}.png')
        plt.savefig(heatmap_path, dpi=150)  # dpi=150：高清图片
        logger.info(f"Temperature heatmap saved to: {heatmap_path}")

    # -------------------------- 2. 温度分布箱线图（按月份展示温度波动） --------------------------
    if 'month' in df.columns and 't2m' in df.columns:
        plt.figure(figsize=(12, 6))
        sns.boxplot(
            data=df,
            x='month',
            y='t2m',
            palette='Set3',
            hue='year',  # 按年份分组（不同年份同月份对比）
            dodge=True  # 分组错开显示，避免重叠
        )
        plt.title(f'{city_name} Temperature Distribution by Month\n{stats_dict["date_range"]}', fontsize=14)
        plt.xlabel('Month')
        plt.ylabel('Average Temperature (°C)')
        plt.xticks(range(0, 12), [f"{i+1}月" for i in range(12)])
        plt.legend(title='Year', bbox_to_anchor=(1.05, 1), loc='upper left')  # 图例放右侧
        plt.tight_layout()
        boxplot_path = os.path.join(viz_dir, f'{city_id}_{city_name}_temp_boxplot_{today}.png')
        plt.savefig(boxplot_path, dpi=150)
        logger.info(f"Temperature boxplot saved to: {boxplot_path}")

    # -------------------------- 3. 统计信息表格图（关键指标汇总） --------------------------
    plt.figure(figsize=(12, 6))
    # 准备表格数据（2列：指标名称、数值）
    table_data = [
        ['Date Range', stats_dict['date_range']],
        ['Total Valid Days', stats_dict['total_days']],
        ['Extreme High Temp (°C)', f"{stats_dict['extreme_high_temp']} ({stats_dict['extreme_high_date']})"],
        ['Extreme Low Temp (°C)', f"{stats_dict['extreme_low_temp']} ({stats_dict['extreme_low_date']})"],
        ['Temp-Humidity Correlation', stats_dict['temp_humidity_corr'] if stats_dict['temp_humidity_corr'] is not None else 'N/A'],
        ['Hottest Month (Avg Temp °C)', f"{stats_dict['hottest_month']} ({stats_dict['hottest_month_temp']})"],
        ['Coldest Month (Avg Temp °C)', f"{stats_dict['coldest_month']} ({stats_dict['coldest_month_temp']})"]
    ]
    # 创建表格（无表头，用第一列作为指标名）
    table = plt.table(
        cellText=table_data,
        cellLoc='left',  # 文本左对齐
        loc='center',    # 表格居中
        colWidths=[0.4, 0.6],  # 列宽比例
        bbox=[0, 0, 1, 1]  # 表格占满整个图
    )
    # 设置表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2)  # 缩放表格（宽x1，高x2，避免拥挤）
    # 设置表头行样式（第一列加粗）
    for i in range(len(table_data)):
        table[(i, 0)].set_facecolor('#f0f0f0')  # 第一列背景色灰色
        table[(i, 0)].set_text_props(weight='bold')  # 第一列文字加粗
    # 隐藏坐标轴（只显示表格）
    plt.axis('off')
    plt.title(f'{city_name} Weather Statistics Summary', fontsize=16, pad=20)
    plt.tight_layout()
    table_path = os.path.join(viz_dir, f'{city_id}_{city_name}_stats_table_{today}.png')
    plt.savefig(table_path, dpi=150, bbox_inches='tight')  # bbox_inches：避免表格被截断
    logger.info(f"Statistics table saved to: {table_path}")

def save_analysis_report(monthly_stats: pd.DataFrame, stats_dict: dict):
    """
    保存分析报告到CSV文件（便于后续Excel分析或分享）
    """
    report_dir = os.path.join(get_output_dir(), 'analysis_reports')
    os.makedirs(report_dir, exist_ok=True)  # 创建报告目录
    today = datetime.now().strftime("%Y%m%d")
    city_name = stats_dict['city_name']
    city_id = stats_dict['city_id']

    # 1. 保存月度统计数据（详细指标）
    monthly_report_path = os.path.join(report_dir, f'{city_id}_{city_name}_monthly_stats_{today}.csv')
    monthly_stats.to_csv(monthly_report_path, index=False, encoding='utf-8-sig')  # utf-8-sig：支持中文Excel打开
    logger.info(f"Monthly stats report saved to: {monthly_report_path}")

    # 2. 保存关键统计信息（简洁版）
    summary_report_path = os.path.join(report_dir, f'{city_id}_{city_name}_stats_summary_{today}.csv')
    summary_df = pd.DataFrame([stats_dict])  # 字典转DataFrame
    summary_df.to_csv(summary_report_path, index=False, encoding='utf-8-sig')
    logger.info(f"Stats summary report saved to: {summary_report_path}")