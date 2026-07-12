from utils.custom_types import PieceworkConfig, PieceworkCosts


def make_piecework_config(**cost_overrides: PieceworkCosts) -> PieceworkConfig:
    costs = {
        "default": PieceworkCosts(
            DA=10.0, ADHDDA=15.0, EVAL=20.0, DAEVAL=25.0, REPORT=5.0
        ),
    }
    costs.update(cost_overrides)
    return PieceworkConfig(
        costs=costs,
        name_map={"jd": "Jane Doe"},
        payroll_emails={"jd": "jane@example.com"},
    )


class TestGetUnitCost:
    def test_falls_back_to_default(self):
        config = make_piecework_config()
        assert config.get_unit_cost("unknown_evaluator", "DA") == 10.0

    def test_uses_evaluator_specific_cost(self):
        config = make_piecework_config(jd=PieceworkCosts(DA=99.0))
        assert config.get_unit_cost("jd", "DA") == 99.0

    def test_evaluator_none_cost_falls_back_to_default(self):
        config = make_piecework_config(jd=PieceworkCosts(DA=None))
        assert config.get_unit_cost("jd", "DA") == 10.0

    def test_unknown_appointment_type_is_zero(self):
        config = make_piecework_config()
        assert config.get_unit_cost("jd", "NOT_A_TYPE") == 0.00


class TestGetFullName:
    def test_case_insensitive_lookup(self):
        config = make_piecework_config()
        assert config.get_full_name("JD") == "Jane Doe"
        assert config.get_full_name("jd") == "Jane Doe"

    def test_unknown_initials_returns_empty_string(self):
        config = make_piecework_config()
        assert config.get_full_name("zz") == ""
