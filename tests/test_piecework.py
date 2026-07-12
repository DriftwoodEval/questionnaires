import pandas as pd

from utils.piecework import extract_writer_initials


class TestExtractWriterInitials:
    def test_strips_non_letters(self):
        assert extract_writer_initials("J.D. 123") == "JD"

    def test_nan_is_empty_string(self):
        assert extract_writer_initials(pd.NA) == ""

    def test_none_is_empty_string(self):
        assert extract_writer_initials(None) == ""

    def test_empty_string_is_empty_string(self):
        assert extract_writer_initials("") == ""
