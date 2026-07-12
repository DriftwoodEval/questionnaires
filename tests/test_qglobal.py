import pytest

from utils.platforms.qglobal import rearrange_dob


class TestRearrangeDob:
    def test_reformats_iso_to_us_style(self):
        assert rearrange_dob("2015/01/02") == "01/02/2015"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            rearrange_dob("01/02/2015")
