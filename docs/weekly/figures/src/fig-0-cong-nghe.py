"""Hình 0: công nghệ và luồng dữ liệu, có logo từng tool.

Cần: Graphviz (dot) trong PATH và `pip install diagrams==0.24.4`.
Chạy từ thư mục figures/src:  python fig-0-cong-nghe.py
Logo MinIO lấy từ Simple Icons (icons/minio.png); icons/delta.png là ký hiệu thay cho Delta Lake.
"""
from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.elastic.elasticsearch import Elasticsearch, Kibana
from diagrams.onprem.analytics import Spark, Superset
from diagrams.onprem.container import Docker
from diagrams.onprem.database import Postgresql
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.logging import Loki
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.network import Internet
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow
from diagrams.programming.language import Python

MAIN = dict(color="#263238", penwidth="2.4")
DATA = dict(color="#546e7a", penwidth="1.5")
CTRL = dict(color="#78909c", style="dashed", penwidth="1.3")
OBS = dict(color="#8d6e63", style="dashed", penwidth="1.5")

graph_attr = {"fontname": "Helvetica", "pad": "0.4", "nodesep": "0.45", "ranksep": "0.75",
              "splines": "spline", "bgcolor": "white", "newrank": "true"}
node_attr = {"fontname": "Helvetica", "fontsize": "14"}
edge_attr = {"fontname": "Helvetica", "fontsize": "13", "fontcolor": "#37474f"}


def box(bg, line):
    return {"bgcolor": bg, "pencolor": line, "fontname": "Helvetica Bold", "fontsize": "17",
            "fontcolor": line, "style": "rounded", "penwidth": "1.6", "margin": "18"}


with Diagram("", filename="fig-0a-pipeline", outformat=["png", "pdf"], show=False, direction="LR",
             graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr):

    with Cluster("Nguồn", graph_attr=box("#eceff1", "#455a64")):
        tiki = Internet("Tiki\nHTML + JSON")
        lazada = Internet("Lazada\nXHR JSON")

    with Cluster("Thu thập", graph_attr=box("#e0f7fa", "#00838f")):
        frontier = Postgresql("frontier\nhàng đợi crawl")
        crawler = Python("crawl-worker\nrequests, Playwright")

    with Cluster("Bronze (MinIO S3)", graph_attr=box("#e3f2fd", "#1565c0")):
        bronze = Custom("raw + records\nJSONL.gz", "icons/minio.png")

    with Cluster("Speed layer (gần thời gian thực)", graph_attr=box("#f3e5f5", "#6a1b9a")):
        kafka = Kafka("Kafka\n5 topic")
        stream = Spark("Structured Streaming\ntrigger 30 s")

    with Cluster("Batch layer (mỗi giờ)", graph_attr=box("#e8f5e9", "#2e7d32")):
        airflow = Airflow("Airflow 3\n4 DAG")
        spark = Spark("Spark batch\n7 stage")
        with Cluster("Lakehouse: Delta Lake trên MinIO", graph_attr=box("#ffffff", "#1565c0")):
            silver = Custom("Silver\nchuẩn hoá", "icons/delta.png")
            gold = Custom("Gold\nstar dw + 12 mart", "icons/delta.png")

    with Cluster("Phục vụ", graph_attr=box("#fff3e0", "#e65100")):
        es = Elasticsearch("Elasticsearch")
        redis = Redis("Redis")
        kibana = Kibana("Kibana\nthay đổi giá")
        pg = Postgresql("PostgreSQL\ncache.*")
        superset = Superset("Superset\nlịch sử giá")

    # thu thập
    tiki >> Edge(**DATA) >> crawler
    lazada >> Edge(**DATA) >> crawler
    frontier >> Edge(label="lease", constraint="false", **CTRL) >> crawler
    tiki >> Edge(style="invis") >> frontier
    crawler >> Edge(label="① ghi trước", **MAIN) >> bronze
    crawler >> Edge(label="② publish", **MAIN) >> kafka

    # speed
    kafka >> Edge(**MAIN) >> stream
    stream >> Edge(label="sự kiện thay đổi", **DATA) >> es
    stream >> Edge(**DATA) >> redis
    es >> Edge(**DATA) >> kibana

    # batch
    airflow >> Edge(label="lập lịch", **CTRL) >> spark
    bronze >> Edge(**MAIN) >> spark
    spark >> Edge(**MAIN) >> silver
    silver >> Edge(**MAIN) >> gold
    gold >> Edge(label="quality PASS", **MAIN) >> pg
    pg >> Edge(**DATA) >> superset



with Diagram("", filename="fig-0b-giam-sat", outformat=["png", "pdf"], show=False, direction="LR",
             graph_attr={**graph_attr, "ranksep": "1.1"}, node_attr=node_attr, edge_attr=edge_attr):
    with Cluster("Giám sát: log và metrics của mọi container", graph_attr=box("#efebe9", "#5d4037")):
        docker = Docker("Docker Compose")
        alloy = Grafana("Grafana Alloy")
        prom = Prometheus("Prometheus")
        loki = Loki("Loki")
        grafana = Grafana("Grafana\n19 cảnh báo")

    # giám sát
    docker >> Edge(**OBS) >> alloy
    alloy >> Edge(label="metrics", **OBS) >> prom
    alloy >> Edge(label="logs", **OBS) >> loki
    prom >> Edge(**OBS) >> grafana
    loki >> Edge(**OBS) >> grafana
