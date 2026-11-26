from app.node import classify_text, estimate_vram_mb


def test_estimate_formula_is_monotonic():
    small = estimate_vram_mb("x" * 10, doc_base_mb=48, mb_per_100_chars=4)
    big = estimate_vram_mb("x" * 6000, doc_base_mb=48, mb_per_100_chars=4)
    assert small == 49  # 48 + ceil(10*4/100)=48+1
    assert big == 288  # 48 + ceil(6000*4/100)=48+240
    assert big > small


def test_classify_keyword_and_fallback_deterministic():
    cat1, conf1 = classify_text(
        "央行与银行发布财报，金融投资向好", "DOC-A"
    )
    assert cat1 == "finance"
    assert 0.0 <= conf1 <= 1.0
    # fallback is deterministic for the same doc id
    c1 = classify_text("zzz qqq no keywords", "DOC-X")[0]
    c2 = classify_text("zzz qqq no keywords", "DOC-X")[0]
    assert c1 == c2
