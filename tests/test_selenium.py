import pytest

from utils.selenium import click_element, find_element, initialize_selenium

SMOKE_PAGE = (
    "data:text/html,"
    "<html><head><title>Smoke Test</title></head>"
    "<body><button id='go' onclick=\"document.getElementById('result').innerText='clicked'\">"
    "Click me</button><p id='result'>waiting</p></body></html>"
)


@pytest.fixture(autouse=True)
def _headless(monkeypatch):
    # Force headless regardless of the ambient environment, so this test never
    # pops up a real browser window on a developer's machine.
    monkeypatch.setenv("HEADLESS", "true")


class TestSeleniumSmoke:
    def test_opens_a_page_and_reads_its_title(self):
        driver = initialize_selenium()
        try:
            driver.get(SMOKE_PAGE)
            assert driver.title == "Smoke Test"
        finally:
            driver.quit()

    def test_finds_and_clicks_an_element(self):
        driver = initialize_selenium()
        try:
            driver.get(SMOKE_PAGE)
            click_element(driver, "id", "go")
            result = find_element(driver, "id", "result")
            assert result.text == "clicked"
        finally:
            driver.quit()
