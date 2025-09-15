import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
import numpy as np
from src.db.mysql_ops import get_db_connection
from config.cities import CITIES

# 设置中文字体
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题

class WeatherVisualizer:
    def __init__(self, root):
        self.root = root
        self.root.title("气象数据可视化工具")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        
        # 存储城市数据
        self.city_data = {}
        self.available_cities = {v[0]: k for k, v in CITIES.items()}  # 城市名: city_id
        self.selected_cities = []
        
        # 创建UI
        self._create_widgets()
        
        # 加载城市列表
        self._load_city_list()
    
    def _create_widgets(self):
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 左侧控制面板
        control_frame = ttk.LabelFrame(main_frame, text="控制面板", padding="10")
        control_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        
        # 城市选择
        ttk.Label(control_frame, text="选择城市:").pack(anchor=tk.W, pady=(0, 5))
        
        self.city_frame = ttk.Frame(control_frame)
        self.city_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.city_listbox = tk.Listbox(self.city_frame, height=15, width=30, selectmode=tk.MULTIPLE)
        self.city_listbox.pack(side=tk.LEFT, fill=tk.Y)
        
        scrollbar = ttk.Scrollbar(self.city_frame, orient=tk.VERTICAL, command=self.city_listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.city_listbox.config(yscrollcommand=scrollbar.set)
        
        # 日期范围选择
        ttk.Label(control_frame, text="选择年份范围:").pack(anchor=tk.W, pady=(10, 5))
        
        year_frame = ttk.Frame(control_frame)
        year_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(year_frame, text="开始年份:").pack(side=tk.LEFT)
        self.start_year = ttk.Combobox(year_frame, values=list(range(2020, 2025)), width=8)
        self.start_year.current(0)
        self.start_year.pack(side=tk.LEFT, padx=(5, 15))
        
        ttk.Label(year_frame, text="结束年份:").pack(side=tk.LEFT)
        self.end_year = ttk.Combobox(year_frame, values=list(range(2020, 2025)), width=8)
        self.end_year.current(4)
        self.end_year.pack(side=tk.LEFT, padx=5)
        
        # 图表类型选择
        ttk.Label(control_frame, text="选择图表类型:").pack(anchor=tk.W, pady=(10, 5))
        
        self.chart_type = tk.StringVar(value="line")
        chart_frame = ttk.Frame(control_frame)
        chart_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Radiobutton(chart_frame, text="折线图", variable=self.chart_type, value="line").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(chart_frame, text="散点图", variable=self.chart_type, value="scatter").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(chart_frame, text="柱状图", variable=self.chart_type, value="bar").pack(side=tk.LEFT, padx=5)
        
        # 数据类型选择
        ttk.Label(control_frame, text="选择数据类型:").pack(anchor=tk.W, pady=(10, 5))
        
        self.data_type = tk.StringVar(value="temp_avg_c")
        data_frame = ttk.Frame(control_frame)
        data_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Radiobutton(data_frame, text="平均温度", variable=self.data_type, value="temp_avg_c").pack(anchor=tk.W)
        ttk.Radiobutton(data_frame, text="最高温度", variable=self.data_type, value="temp_max_c").pack(anchor=tk.W)
        ttk.Radiobutton(data_frame, text="最低温度", variable=self.data_type, value="temp_min_c").pack(anchor=tk.W)
        
        # 按钮
        btn_frame = ttk.Frame(control_frame)
        btn_frame.pack(fill=tk.X, pady=(20, 10))
        
        ttk.Button(btn_frame, text="加载数据", command=self._load_data).pack(fill=tk.X, pady=(0, 5))
        ttk.Button(btn_frame, text="生成图表", command=self._generate_chart).pack(fill=tk.X, pady=(5, 5))
        ttk.Button(btn_frame, text="清除图表", command=self._clear_chart).pack(fill=tk.X, pady=(5, 0))
        
        # 右侧图表区域
        self.chart_frame = ttk.LabelFrame(main_frame, text="数据图表", padding="10")
        self.chart_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # 创建图表
        self.fig, self.ax = plt.subplots(figsize=(8, 6))
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # 状态栏
        self.status_var = tk.StringVar(value="就绪")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def _load_city_list(self):
        """加载城市列表到列表框"""
        cities = sorted(self.available_cities.keys())
        for city in cities:
            self.city_listbox.insert(tk.END, city)
    
    def _load_data(self):
        """从数据库加载选中城市的数据"""
        # 获取选中的城市
        selected_indices = self.city_listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("警告", "请至少选择一个城市")
            return
        
        self.selected_cities = [self.city_listbox.get(i) for i in selected_indices]
        start_year = int(self.start_year.get())
        end_year = int(self.end_year.get())
        
        if start_year > end_year:
            messagebox.showwarning("警告", "开始年份不能大于结束年份")
            return
        
        self.status_var.set(f"正在加载 {', '.join(self.selected_cities)} 的数据...")
        self.root.update()
        
        try:
            # 连接数据库
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 清空现有数据
            self.city_data.clear()
            
            # 为每个选中的城市加载数据
            for city_name in self.selected_cities:
                city_id = self.available_cities[city_name]
                
                # 查询数据
                query = """
                SELECT date, temp_max_c, temp_min_c, temp_avg_c
                FROM weather_daily
                WHERE city_id = %s 
                  AND YEAR(date) BETWEEN %s AND %s
                ORDER BY date
                """
                cursor.execute(query, (city_id, start_year, end_year))
                data = cursor.fetchall()
                
                if not data:
                    messagebox.showinfo("提示", f"未找到 {city_name} 在 {start_year}-{end_year} 年的数据")
                    continue
                
                # 转换为DataFrame
                df = pd.DataFrame(data, columns=['date', 'temp_max_c', 'temp_min_c', 'temp_avg_c'])
                df['date'] = pd.to_datetime(df['date'])
                self.city_data[city_name] = df
            
            conn.close()
            
            if self.city_data:
                self.status_var.set(f"已加载 {len(self.city_data)} 个城市的数据")
                messagebox.showinfo("成功", f"已成功加载 {', '.join(self.city_data.keys())} 的数据")
            else:
                self.status_var.set("未加载到任何数据")
                
        except Exception as e:
            self.status_var.set("数据加载失败")
            messagebox.showerror("错误", f"加载数据时发生错误: {str(e)}")
    
    def _generate_chart(self):
        """生成选中类型的图表"""
        if not self.city_data:
            messagebox.showwarning("警告", "请先加载数据")
            return
        
        # 清除现有图表
        self.ax.clear()
        
        # 获取用户选择
        chart_type = self.chart_type.get()
        data_type = self.data_type.get()
        
        # 数据类型中文名称
        data_labels = {
            'temp_avg_c': '平均温度 (℃)',
            'temp_max_c': '最高温度 (℃)',
            'temp_min_c': '最低温度 (℃)'
        }
        
        # 为每个城市绘制数据
        colors = ['blue', 'green', 'red', 'purple', 'orange', 'cyan', 'magenta']
        markers = ['o', 's', '^', 'D', 'v', '<', '>']
        
        for i, (city_name, df) in enumerate(self.city_data.items()):
            color = colors[i % len(colors)]
            marker = markers[i % len(markers)]
            
            x_data = df['date']
            y_data = df[data_type]
            
            if chart_type == 'line':
                self.ax.plot(x_data, y_data, label=city_name, color=color, marker=marker, markersize=3, alpha=0.7)
            elif chart_type == 'scatter':
                self.ax.scatter(x_data, y_data, label=city_name, color=color, marker=marker, s=10, alpha=0.7)
            elif chart_type == 'bar':
                # 柱状图使用月度平均值
                df['month'] = df['date'].dt.to_period('M')
                monthly_avg = df.groupby('month')[data_type].mean()
                self.ax.bar(monthly_avg.index.astype(str), monthly_avg.values, 
                           label=city_name, color=color, alpha=0.7, width=0.8)
        
        # 设置图表属性
        self.ax.set_title(f"{', '.join(self.selected_cities)} {data_labels[data_type]}趋势 ({self.start_year.get()}-{self.end_year.get()})")
        self.ax.set_xlabel('日期')
        self.ax.set_ylabel(data_labels[data_type])
        self.ax.grid(True, alpha=0.3)
        
        # 自动旋转x轴标签以防重叠
        plt.xticks(rotation=45, ha='right')
        
        # 添加图例
        if len(self.city_data) > 1:
            self.ax.legend(loc='best')
        
        # 调整布局
        self.fig.tight_layout()
        
        # 更新画布
        self.canvas.draw()
        
        self.status_var.set(f"已生成{len(self.city_data)}个城市的{chart_type}图表")
    
    def _clear_chart(self):
        """清除当前图表"""
        self.ax.clear()
        self.canvas.draw()
        self.status_var.set("图表已清除")

if __name__ == "__main__":
    root = tk.Tk()
    app = WeatherVisualizer(root)
    root.mainloop()
