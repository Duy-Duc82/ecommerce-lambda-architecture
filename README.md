# 📊 Big Data Lambda Architecture — E-Commerce Analytics

Nền tảng phân tích dữ liệu lớn cho hệ thống **Thương mại Điện tử (TMĐT)** sử dụng **Kiến trúc Lambda**.

## 🏗️ Kiến trúc Hybrid Enterprise Analytics

Dự án áp dụng **Lambda Architecture** kết hợp phân tách vai trò Dashboard (SOC vs BI):

```text
Producers → Kafka → Speed Layer (Spark Streaming → Redis/Elasticsearch)
                  → MinIO (Data Lake) → Batch Layer (PySpark + ML) → Postgres DW
                                                                   
[ SPEED LAYER / SOC ]                       [ SERVING LAYER / BI ]
Elasticsearch ──► Kibana                    PostgreSQL ──► Apache Superset
(Real-time, Fraud, Anomalies)               (Business BI, Trends, Forecasts)
```

### Các thành phần chính

| Layer | Công nghệ | Mô tả |
|-------|-----------|-------|
| **Ingestion** | Kafka | Message broker, 3 topics (events, orders, prices) |
| **Data Lake** | MinIO (S3) | Lưu trữ raw data dạng JSON/Parquet |
| **Speed Layer** | Spark Streaming | Xử lý real-time, index vào Elasticsearch |
| **Batch Layer** | PySpark | ETL + 4 ML models chạy định kỳ |
| **Serving DW** | Postgres | Data Warehouse chứa aggregated data & ML views |
| **SOC Dashboard** | Kibana | Giám sát Real-time, Security, Anomaly (Dark mode) |
| **BI Dashboard** | Superset | Phân tích doanh thu, hành vi, báo cáo kinh doanh |

### 4 Bài toán ML

1. **Phân tích xu hướng** — Moving Average, Growth Rate, Category Trends
2. **Phát hiện bất thường** — Z-score, IQR, Spike/Drop Detection
3. **Dự báo biến động giá** — SMA/EMA Forecast, Price Volatility, Elasticity
4. **Phát hiện gian lận** — Rule-based + Isolation Forest

## 🚀 Khởi động nhanh

### Yêu cầu
- Docker Desktop (8GB+ RAM)
- Python 3.12+
- Java 17+ (cho PySpark)

### 1. Cài đặt dependencies

```powershell
# Tạo virtual environment
python -m venv .venv
.\.venv\Scripts\activate

# Cài đặt packages
pip install -r requirements.txt
```

### 2. Khởi động infrastructure

```powershell
# Core services (Kafka, Spark, MinIO, Redis, Postgres)
docker compose up -d

# Full stack (thêm Elasticsearch + Kibana)
docker compose --profile full up -d
```

### 3. Tạo Kafka Topics

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic ecommerce_events --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic ecommerce_orders --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic ecommerce_prices --partitions 1 --replication-factor 1 --if-not-exists
```

### 4. Chạy Data Producer (Terminal 1)

```powershell
# Test mode (in ra console)
python -m data_ingestion.producer --test-mode -n 20

# Normal mode (gửi vào Kafka)
python -m data_ingestion.producer --eps 50

# Black Friday simulation
python -m data_ingestion.producer --burst
```

### 5. Chạy Speed Layer (Terminal 2)

```powershell
# Local mode
python speed_layer/speed_layer.py

# Cluster mode
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 speed_layer/speed_layer.py
```

### 6. Chạy Batch Layer

```powershell
# ETL
python -m batch_layer.etl_job --source ./sample_data/

# ML Models
python -m batch_layer.models.trend_analysis
python -m batch_layer.models.anomaly_detection
python -m batch_layer.models.price_forecast
python -m batch_layer.models.fraud_detection

# Batch Views
python -m batch_layer.batch_views
```

### 7. Dashboard

```powershell
streamlit run dashboard/app.py
```

## 📁 Cấu trúc dự án

```
├── config/                  # Config tập trung
│   └── settings.py
├── data_ingestion/          # Kafka producers + schemas
│   ├── producer.py
│   ├── schemas.py
│   └── sample_data/
├── batch_layer/             # Batch ETL + ML models
│   ├── etl_job.py
│   ├── batch_views.py
│   └── models/
│       ├── trend_analysis.py
│       ├── anomaly_detection.py
│       ├── price_forecast.py
│       └── fraud_detection.py
├── speed_layer/             # Real-time processing
│   └── speed_layer.py
├── serving_layer/           # Postgres views + Redis cache
│   ├── postgres_views.py
│   └── redis_cache.py
├── dashboard/               # Streamlit dashboard
│   └── app.py
├── scripts/                 # DevOps scripts
├── tests/                   # Unit tests
├── docker-compose.yml
├── requirements.txt
└── .env
```

## 🔗 Service URLs

| Service | URL |
|---------|-----|
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| MinIO Console | http://localhost:9001 |
| Kibana (SOC) | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |
| Apache Superset (BI) | http://localhost:8088 |

## 📊 Nguồn dữ liệu (Kaggle Dataset)

Dự án sử dụng bộ dữ liệu hành vi mua sắm thực tế từ Kaggle cho phần Ingestion:
- **Link Dataset:** [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- **Lưu ý cài đặt:** Do dung lượng file gốc rất lớn nên các thư mục chứa data đã được bỏ qua (ignore) trên GitHub để tránh lỗi. Để có thể chạy được mô phỏng (`demo_phase1`), bạn vui lòng tải dataset từ link trên, giải nén và đặt các file `.csv` (`2019-Oct.csv`, `2019-Nov.csv`) vào đường dẫn: `demo_phase1/data_kaggle/`.

## 📊 Dữ liệu giả lập

- **100 sản phẩm** thuộc 20 danh mục (Điện thoại, Laptop, Thời trang, Gia dụng...)
- **1000 users** mô phỏng
- **6 loại events**: page_view (50%), add_to_cart (20%), purchase (15%), review (10%), price_change (3%), search (2%)
- Hỗ trợ chế độ **burst** mô phỏng Black Friday
