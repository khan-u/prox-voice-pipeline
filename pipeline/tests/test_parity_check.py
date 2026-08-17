from pipeline.validate.parity_check import (
    compare_values,
    reference_dtx,
    reference_sensing,
)


def test_compare_values_tolerance():
    assert compare_values(202, 203) is True     # within +-1
    assert compare_values(202, 205) is False
    assert compare_values(None, None) is True
    assert compare_values(5, None) is False


def test_reference_dtx_medians_by_group():
    records = [
        {"source": "speech", "dtx": True, "uplinkKbps": 18.1},
        {"source": "speech", "dtx": True, "uplinkKbps": 17.9},
        {"source": "speech", "dtx": False, "uplinkKbps": 23.0},
        {"source": "tone", "dtx": False, "uplinkKbps": -1},   # unmeasured, dropped
    ]
    ref = reference_dtx(records)
    assert ref[("speech", True)] == 18   # median(18.1,17.9)=18.0 -> 18
    assert ref[("speech", False)] == 23
    assert ("tone", False) not in ref


def test_reference_sensing_pools_clients():
    records = [{
        "scenario": "idle",
        "clients": {
            "wsn0": {"stats": {"suppressedPct": 91}},
            "wsn1": {"stats": {"suppressedPct": 89}},
        },
    }]
    assert reference_sensing(records) == {"idle": 90}   # median(91,89)=90
