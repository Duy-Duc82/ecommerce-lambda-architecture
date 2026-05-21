import argparse
import csv
import json
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# Thêm project root vào sys.path để import config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    KAFKA_TOPIC_ORDERS,
)
from data_ingestion.producer import create_producer, _get_key_for_event
from data_ingestion.schemas import (
    AddToCartEvent,
    PageViewEvent,
    PurchaseEvent,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def parse_time(time_str: str) -> int:
    """Chuyển đổi thời gian từ Kaggle CSV sang timestamp."""
    # Định dạng thường thấy trong Kaggle dataset: '2019-10-01 00:00:00 UTC'
    try:
        # Bỏ đi đuôi UTC để parse dễ hơn
        time_str = time_str.replace(" UTC", "")
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    except Exception:
        return int(time.time())

def map_kaggle_row_to_event(row: dict) -> dict | None:
    """Chuyển đổi 1 dòng trong file CSV thành Event của dự án."""
    event_type = row.get("event_type", "")
    
    # Kaggle struct: event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
    timestamp = parse_time(row.get("event_time", ""))
    user_id = str(row.get("user_id", "unknown_user"))
    product_id = str(row.get("product_id", "unknown_product"))
    
    # Xử lý category và name
    category = str(row.get("category_code", "unknown_category"))
    brand = str(row.get("brand", ""))
    product_name = f"{brand} {product_id}".strip() # Giả lập tên = brand + ID
    
    try:
        price = float(row.get("price", 0.0))
    except ValueError:
        price = 0.0

    if event_type == "view":
        return PageViewEvent(
            user_id=user_id,
            product_id=product_id,
            product_name=product_name,
            category=category,
            timestamp=timestamp,
        ).to_dict()

    elif event_type == "cart":
        return AddToCartEvent(
            user_id=user_id,
            product_id=product_id,
            product_name=product_name,
            category=category,
            quantity=1, # Kaggle dataset từng dòng là 1 item
            price=price,
            timestamp=timestamp,
        ).to_dict()

    elif event_type == "purchase":
        # Kaggle dataset purchase ghi nhận từng sản phẩm độc lập
        return PurchaseEvent(
            user_id=user_id,
            order_id=f"KAGGLE-ORD-{uuid.uuid4().hex[:8].upper()}",
            product_ids=[product_id],
            product_names=[product_name],
            quantities=[1],
            total_amount=price,
            payment_method="kaggle_imported",
            timestamp=timestamp,
        ).to_dict()

    return None

def get_topic_for_kaggle_event(event: dict) -> str:
    """Xác định Kafka topic."""
    if event.get("event_type") == "purchase":
        return KAFKA_TOPIC_ORDERS
    return KAFKA_TOPIC_EVENTS

def run_kaggle_producer(csv_file_path: str, events_per_second: float, export_json_path: str | None = None):
    file_path = Path(csv_file_path)
    if not file_path.exists():
        logger.error("Không tìm thấy file %s", file_path)
        sys.exit(1)

    export_handle = None
    if export_json_path:
        export_path = Path(export_json_path)
        export_path.parent.mkdir(parents=True, exist_ok=True)
        export_handle = export_path.open("a", encoding="utf-8")
        logger.info("Export JSONL raw events -> %s", export_path)

    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
    interval = 1.0 / events_per_second if events_per_second > 0 else 0
    total_sent = 0

    logger.info("Bắt đầu đọc dữ liệu từ %s...", file_path)
    try:
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                event = map_kaggle_row_to_event(row)
                if not event:
                    continue
                
                topic = get_topic_for_kaggle_event(event)
                key = _get_key_for_event(event)

                producer.send(topic, value=event, key=key)
                if export_handle:
                    export_handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                total_sent += 1

                if total_sent % 1000 == 0:
                    producer.flush()
                    logger.info("Đã gửi %d events từ Kaggle", total_sent)

                if interval > 0:
                    time.sleep(interval)
                    
    except KeyboardInterrupt:
        logger.info("Dừng chạy giữa chừng. Tổng events đã gửi: %d", total_sent)
    finally:
        producer.flush()
        producer.close()
        if export_handle:
            export_handle.close()
        logger.info("Hoàn tất! Tổng events: %d", total_sent)


def main():
    parser = argparse.ArgumentParser(description="Kaggle E-commerce Data Importer")
    parser.add_argument("--csv", required=True, help="Đường dẫn đến file CSV ví dụ dataset Kaggle")
    parser.add_argument("--eps", type=int, default=100, help="Số events gửi mỗi giây (mặc định: 100)")
    parser.add_argument("--export-json", default=None, help="Đường dẫn JSONL để phục vụ batch ETL")
    args = parser.parse_args()

    run_kaggle_producer(args.csv, args.eps, export_json_path=args.export_json)


if __name__ == "__main__":
    main()
