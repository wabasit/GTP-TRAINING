def test_valid_rate():
    assert 60 <= 80 <= 120

def test_anomaly_detection():
    from consumer import is_anomalous
    assert is_anomalous(45)
    assert not is_anomalous(85)