import asyncio

from app.node import SimulatedClassificationNode


class FakeDB:
    def __init__(self, delay_ms=0):
        self._s = {"vram_limit_mb": 512, "per_doc_delay_ms": delay_ms}

    def get_node_settings(self):
        return dict(self._s)


def _doc(seq, est, doc_id=None):
    return {
        "seq": seq,
        "doc_id": doc_id or f"D{seq}",
        "content": "x",
        "vram_estimate_mb": est,
    }


def test_node_returns_partial_results_then_oom():
    node = SimulatedClassificationNode(FakeDB(), model_resident_mb=128)
    docs = [_doc(0, 50), _doc(1, 50), _doc(2, 300), _doc(3, 50)]

    result = asyncio.run(node.classify_batch(docs, 512))

    assert [d["seq"] for d in result.succeeded] == [0, 1]
    assert result.oom_occurred
    assert result.oom.next_seq == 2
    assert result.oom.remaining_seqs == [2, 3]
    # used before doc2 = resident + two docs; doc2 tips over the cap
    assert result.oom.used_before_mb == 228
    assert result.oom.required_total_mb == 528
    assert result.oom.limit_mb == 512


def test_node_single_oversized_doc_oom_with_zero_partial():
    node = SimulatedClassificationNode(FakeDB(), model_resident_mb=128)
    result = asyncio.run(node.classify_batch([_doc(7, 900)], 512))
    assert result.succeeded == []
    assert result.oom_occurred
    assert result.oom.next_seq == 7
    assert result.oom.required_total_mb == 1028


def test_node_all_fit_no_oom():
    node = SimulatedClassificationNode(FakeDB(), model_resident_mb=128)
    docs = [_doc(i, 40) for i in range(4)]
    result = asyncio.run(node.classify_batch(docs, 512))
    assert len(result.succeeded) == 4
    assert result.oom is None
    assert all(r["category"] for r in result.succeeded)
