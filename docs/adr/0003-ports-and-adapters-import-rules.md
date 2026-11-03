# ADR-0003: Ports & Adapters và luật import

Trạng thái: chấp nhận (2026-10-24). Kiểm bằng `import-linter` (`pyproject.toml`) và `tests/architecture/`.

## Bối cảnh

Code cũ có phụ thuộc vòng và sai chiều: `common` và `config` import lẫn nhau, crawler import ngược vào ingestion, module chung import thẳng site cụ thể, `settings.py` chạy `load_dotenv` và validate ngay lúc import. Hệ quả là không inject được fake, mỗi test phải monkeypatch.

## Quyết định

Các tầng, từ trên xuống:

1. `orchestration` | `ops` | `serving`
2. `ingestion` | `processing` (độc lập, không import lẫn nhau)
3. `infra`
4. `settings`
5. `domain`

Luật (contract import-linter):

- `layers`: tầng trên được import tầng dưới, không ngược lại.
- `independence`: `ecomdata.ingestion` và `ecomdata.processing`.
- `forbidden`: `ecomdata.domain` không import `pyspark`, `requests`, `psycopg2`, `kafka`, `minio`, `boto3`, `playwright`, `redis`, `elasticsearch`, `ecomdata.settings`, `ecomdata.infra`.
- `forbidden`: `ecomdata` không import `legacy`.
- `tests/architecture/test_import_rules.py`: settings chỉ được đọc ở composition root (`bootstrap.py`, `cli.py`, `service.py`, `dags/*`).
- `tests/architecture/test_site_literals.py`: literal riêng của site (`"tiki"`, `"lazada"`, `tiki.vn`, `lazada.vn`) chỉ nằm trong `ingestion/sources/<site>/`, `processing/normalize/sources/<site>.py` và `domain/sources.py`.

Cách làm:

- Logic nhận **Protocol** (`processing/batch/ports.py`: `ObjectStore`, `AuditRepo`, `QualityRepo`, `CachePublisher`, `LakeIO`); composition root (`bootstrap.py`) inject adapter thật, test inject bản trong bộ nhớ (`processing/batch/memory.py`, `tests/spark/lake_support.py`).
- `settings.load_settings(env)` thuần: không đọc môi trường lúc import; `dotenv=True` chỉ do composition root yêu cầu.
- Một chỗ duy nhất cho: `SourceRegistry`, `SparkSessionFactory` (`infra/spark.py`), `jdbc_url` (`infra/postgres.py`), producer Kafka (`infra/kafka.py`), helper validate (`domain/validation.py`), quy tắc thứ tự (`domain/ordering.py`); kiểm bằng `tests/architecture/test_single_definitions.py`.

## Hệ quả

- Test đơn vị không cần mạng, Spark hay Postgres (test runner và stage publish/promote dùng bộ nhớ).
- Thêm adapter (ví dụ `ApiFetcher`) không đụng domain.
- Vi phạm luật làm `lint-imports` hoặc test kiến trúc đỏ ngay trong suite.
