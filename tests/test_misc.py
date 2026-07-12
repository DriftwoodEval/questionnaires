from datetime import date, timedelta

from utils.misc import check_distance


class TestCheckDistance:
    def test_today_is_zero(self):
        assert check_distance(date.today()) == 0

    def test_past_date_is_positive(self):
        assert check_distance(date.today() - timedelta(days=5)) == 5

    def test_future_date_is_negative(self):
        assert check_distance(date.today() + timedelta(days=3)) == -3
