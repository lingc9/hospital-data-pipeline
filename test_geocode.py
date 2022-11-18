"""Unit test for format_geocode"""

from cleandata import format_geocode


def test_inputs():
    code = "POINT (-90.60539 34.50271)"
    long, lat = format_geocode(code)
    assert long == -90.60539
    assert lat == 34.50271

    code = "POINT (-104.9845 39.86415)"
    long, lat = format_geocode(code)
    assert long == -104.9845
    assert lat == 39.86415

    code = "POINT (-92.317039 38.950669)"
    long, lat = format_geocode(code)
    assert long == -92.317039
    assert lat == 38.950669
