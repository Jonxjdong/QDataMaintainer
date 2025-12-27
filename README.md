# QDataMaintainer

QDataMaintainer 是一个用于维护本地 A 股市场数据的轻量级工具。它主要用于获取和存储 **5分钟频率** 的股票行情数据。

## 功能特点

- **数据源**: 使用 [Baostock](http://baostock.com/) 作为免费数据源。
- **存储格式**: 使用 Parquet 格式存储数据，按日期分片（每天一个文件），兼顾读取性能和压缩率。
- **增量更新**: 自动记录最后更新日期，支持增量获取最新数据。
- **元数据管理**: 使用 SQLite (`data/meta.db`) 管理股票列表和交易日历。

## 环境依赖

本项目依赖 Python 3.11+，主要依赖库如下：
- `baostock`: 数据接口
- `pandas`: 数据处理
- `pyarrow`: Parquet 文件读写
- `sqlalchemy`: 数据库交互

安装依赖：
```bash
pip install -r requirements.txt
```

## 使用说明

### 更新日常数据

使用 `run_daily_update.py` 脚本进行数据更新。默认情况下，它会从上次成功的日期继续更新到今天。

```bash
python run_daily_update.py
```

### 指定日期范围

你可以通过命令行参数强制指定更新的起止日期：

```bash
# 更新指定日期的数据
python run_daily_update.py --start_date 2025-01-01 --end_date 2025-01-31
```

- `--start_date`: 开始日期 (YYYY-MM-DD)
- `--end_date`: 结束日期 (YYYY-MM-DD)

## 数据目录结构

运行后，数据将保存在 `data/` 目录下：

```
data/
├── min5/               # 5分钟线数据
│   ├── 2025-01-01.parquet
│   ├── 2025-01-02.parquet
│   └── ...
├── daily/              # 日线数据（预留）
└── meta.db             # 元数据库 (SQLite)
```