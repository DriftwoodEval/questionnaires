from unittest.mock import patch

from utils.task_tracker import track_task


def test_track_task_connects_with_the_given_config(config_factory):
    config = config_factory()
    with (
        patch("utils.task_tracker.get_db", return_value="conn") as get_db,
        patch("utils.task_tracker._track_task") as shared,
    ):
        track_task(config, "qsend", "Sending", exclusive=False, dev_mode=True)

        factory, task_type, label, exclusive = shared.call_args.args
        assert (task_type, label, exclusive) == ("qsend", "Sending", False)
        assert shared.call_args.kwargs == {"dev_mode": True}
        assert factory() == "conn"
        get_db.assert_called_once_with(config)
