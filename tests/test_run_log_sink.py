"""RunLogSink batches log records and tags them with the current client."""

from unittest.mock import patch

from loguru import logger

from utils.misc import RunLogSink, set_log_client


def test_records_are_posted_with_client_id():
    with patch("utils.misc.requests.post") as post:
        sink = RunLogSink("qsend", "https://site.example/", "secret")
        handler_id = logger.add(sink.write, level="DEBUG")
        try:
            logger.info("no client yet")
            set_log_client(42)
            logger.warning("client line")
            set_log_client(None)
        finally:
            logger.remove(handler_id)
            sink.close()

    lines = [
        line for call in post.call_args_list for line in call.kwargs["json"]["lines"]
    ]
    assert [(line["message"], line["clientId"]) for line in lines] == [
        ("no client yet", None),
        ("client line", 42),
    ]
    first = post.call_args_list[0]
    assert first.args[0] == "https://site.example/api/internal/run-logs"
    assert first.kwargs["json"]["app"] == "qsend"
