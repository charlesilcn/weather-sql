#NASA 气象数据抓取与管理系统

[![Python Version](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0%2B-green.svg)](https://www.mysql.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

从 NASA POWER 接口批量抓取全球 294 座城市的气象数据（2020-2024 年），支持断点续传、数据清洗和 MySQL 高效入库，附带图形化操作界面。


## ✨ 核心功能

- **全量数据覆盖**：294 座城市的每日最高温、最低温、平均温数据（2020-2024 年）
- **智能抓取机制**：
  - 分段请求（按季度拆分，减少单次请求压力）
  - 断点续传（已下载片段自动跳过，网络中断后可恢复）
  - 自动重试（网络异常时指数退避重试，提高成功率）
- **数据质量保障**：自动过滤 NASA 缺测值（-999），确保入库数据有效性
- **高效入库**：通过 MySQL `LOAD DATA` 批量导入，比单条插入快 10 倍以上
- **图形化操作**：双击即可运行的合并+入库工具，弹窗展示结果


## 📊 数据说明

| 字段名         | 含义               | 单位   | 来源字段（NASA） |
|----------------|--------------------|--------|------------------|
| `city_id`      | 城市唯一标识       | -      | -                |
| `date`         | 日期               | YYYY-MM-DD | -             |
| `temp_max_c`   | 每日最高温度       | ℃      | `T2M_MAX`        |
| `temp_min_c`   | 每日最低温度       | ℃      | `T2M_MIN`        |
| `temp_avg_c`   | 每日平均温度       | ℃      | `T2M`            |


## 🚀 快速开始

### 环境要求
- Python 3.8+
- MySQL 8.0+（需开启 `local_infile` 权限）
- 依赖库：`pandas`, `requests`, `pymysql`, `tqdm`