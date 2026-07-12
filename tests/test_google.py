import pytest

from utils.google import col_index_to_a1


class TestColIndexToA1:
    @pytest.mark.parametrize(
        ("col_index", "expected"),
        [
            (0, "A"),
            (25, "Z"),
            (26, "AA"),
            (51, "AZ"),
            (52, "BA"),
        ],
    )
    def test_col_index_to_a1(self, col_index, expected):
        assert col_index_to_a1(col_index) == expected
