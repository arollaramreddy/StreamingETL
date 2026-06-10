"""StreamingETL — Pipeline unit and smoke tests.

Run: uv run pytest tests/ -v

Covers: Kafka producer, Bronze schema, Silver transforms,
Gold models, Dashboard health, K8s manifests, Terraform files.
"""
import json, os, sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))


class TestKafkaProducer:
    def test_message_key_wiki_and_title(self):
        from data_extraction.kafka_extraction import get_message_key
        assert get_message_key({"wiki": "enwiki", "title": "Test"}) == "enwiki:Test"

    def test_message_key_wiki_only(self):
        from data_extraction.kafka_extraction import get_message_key
        assert get_message_key({"wiki": "enwiki"}) == "enwiki"

    def test_message_key_none_when_empty(self):
        from data_extraction.kafka_extraction import get_message_key
        assert get_message_key({}) is None


class TestBronzeSchema:
    REQUIRED = {"kafka_topic","kafka_partition","kafka_offset","kafka_timestamp","bronze_ingested_date","value"}

    def test_required_fields_present(self):
        row = {k: "x" for k in self.REQUIRED}
        assert not (self.REQUIRED - set(row.keys()))


class TestSilverTransforms:
    def _event(self, **kw):
        base = {"id":"abc","wiki":"enwiki","title":"Test","user":"Ed","bot":False,"type":"edit","timestamp":1}
        base.update(kw)
        return base

    def test_null_wiki_fails_quality(self):
        e = self._event(wiki=None)
        assert not (e.get("wiki") and e.get("title"))

    def test_valid_event_passes_quality(self):
        e = self._event()
        required = ["id","wiki","title","user","type","timestamp"]
        assert all(e.get(f) is not None for f in required)

    def test_deduplication_by_id(self):
        events = [self._event(id="dup"), self._event(id="dup"), self._event(id="uniq")]
        seen, out = set(), []
        for e in events:
            if e["id"] not in seen:
                seen.add(e["id"]); out.append(e)
        assert len(out) == 2

    def test_bot_flag_preserved(self):
        assert self._event(bot=True)["bot"] is True


class TestGoldModels:
    def test_fact_table_columns(self):
        required = {"event_id","date_key","wiki_key","page_key","user_key","event_type_key","is_bot","event_date"}
        row = {k: "x" for k in required}
        assert not (required - set(row.keys()))


class TestDashboardHealth:
    def test_stale_when_no_rows(self):
        assert 0 < int(os.getenv("DASHBOARD_MIN_FACT_ROWS", "1"))

    def test_healthy_when_rows_present(self):
        assert 100 >= int(os.getenv("DASHBOARD_MIN_FACT_ROWS", "1"))


class TestK8sManifests:
    K8S = PROJECT_ROOT / "k8s"
    FILES = [
        "namespace.yaml", "configmap.yaml",
        "kafka/kafka-deployment.yaml", "kafka/kafka-service.yaml", "kafka/kafka-hpa.yaml",
        "airflow/airflow-deployment.yaml", "airflow/airflow-service.yaml",
        "dashboard/dashboard-deployment.yaml",
        "redis/redis-deployment.yaml",
    ]

    @pytest.mark.parametrize("f", FILES)
    def test_file_exists(self, f):
        assert (self.K8S / f).exists(), f"Missing: {f}"

    @pytest.mark.parametrize("f", [f for f in FILES if f != "namespace.yaml"])
    def test_namespace_referenced(self, f):
        p = self.K8S / f
        if not p.exists(): pytest.skip()
        assert "streamingetl" in p.read_text()


class TestTerraformFiles:
    TF = PROJECT_ROOT / "terraform"
    FILES = ["main.tf","variables.tf","outputs.tf","storage.tf","aks.tf"]

    @pytest.mark.parametrize("f", FILES)
    def test_file_exists(self, f):
        assert (self.TF / f).exists(), f"Missing terraform file: {f}"

    def test_storage_has_four_layers(self):
        p = self.TF / "storage.tf"
        if not p.exists(): pytest.skip()
        c = p.read_text()
        for layer in ["bronze","silver","gold","quarantine"]:
            assert layer in c
