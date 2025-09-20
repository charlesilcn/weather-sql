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