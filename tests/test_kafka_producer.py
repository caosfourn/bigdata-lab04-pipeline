from pathlib import Path

from src.kafka_producer import event_key, iter_selected_files, publish_file


class CompletedFuture:
    def get(self, timeout=None):
        return {"timeout": timeout}


class RecordingProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        return CompletedFuture()


def test_event_key_prefers_entity_ids():
    assert event_key({"node_id": "node-1", "file_path": "x.py"}) == "node-1"
    assert event_key({"edge_id": "edge-1", "file_path": "x.py"}) == "edge-1"
    assert event_key({"file_path": "x.py"}) == "x.py"


def test_publish_file_routes_nodes_edges_and_metadata(tmp_path: Path):
    source = tmp_path / "sample.py"
    source.write_text("def answer():\n    return 42\n", encoding="utf-8")
    producer = RecordingProducer()

    result = publish_file(producer, source, tmp_path)

    topics = [topic for topic, _, _ in producer.sent]
    assert "cpg.nodes" in topics
    assert "cpg.edges" in topics
    assert topics.count("cpg.metadata") == 1
    assert "cpg.errors" not in topics
    assert result["error"] is None


def test_iter_selected_files_validates_single_file(tmp_path: Path):
    source = tmp_path / "sample.py"
    source.write_text("x = 1\n", encoding="utf-8")
    assert list(iter_selected_files(tmp_path, source)) == [source.resolve()]
