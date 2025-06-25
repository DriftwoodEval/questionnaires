import time

from loguru import logger
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver

import shared_utils as utils
from qsend import login_ta

services, config = utils.load_config()


def open_profile(driver: WebDriver):
    logger.debug("Opening profile")
    utils.click_element(driver, By.CLASS_NAME, "user-menu-button")
    utils.click_element(
        driver,
        By.XPATH,
        "//span[contains(normalize-space(text()), 'Your Profile')]",
    )


def export_data(driver: WebDriver):
    def _helper(driver: WebDriver, data_title: str) -> bool:
        logger.debug(f"Exporting {data_title}")
        try:
            utils.click_element(
                driver,
                By.XPATH,
                f"//h5[contains(normalize-space(text()), '{data_title}')]/following-sibling::p/a[contains(text(), 'Re-Export')]",
            )
            return True
        except NoSuchElementException:
            try:
                logger.error(
                    f"Could not find {data_title} Re-Export button, has it never been started before?"
                )
                utils.click_element(
                    driver,
                    By.XPATH,
                    f"//h5[contains(normalize-space(text()), '{data_title}')]/following-sibling::p/a[contains(text(), 'Start')]",
                )
                return True
            except NoSuchElementException:
                logger.error(f"Could not find {data_title} Start button")
                return False

    driver.get(driver.current_url + "#therapist-data-export")

    started = _helper(driver, "Client Appointments")
    if not started:
        return
    utils.click_element(driver, By.CSS_SELECTOR, "[data-dismiss='modal']")
    _helper(driver, "Clients")
    utils.click_element(driver, By.CSS_SELECTOR, "[data-dismiss='modal']")
    _helper(driver, "Insurance Policies and Benefits")
    utils.click_element(driver, By.CSS_SELECTOR, "[data-dismiss='modal']")


def loop_therapists(driver: WebDriver):
    def _helper(driver: WebDriver, count: int) -> int:
        therapist_element = utils.find_element(
            driver, By.CSS_SELECTOR, f"#nav-staff-menu>ul>li:nth-child({count + 1})>a"
        )
        if any(s in therapist_element.text for s in config.excluded_ta):
            logger.debug(f"Skipping therapist: {therapist_element.text}")
            count += 1
            return count
        print(count)
        logger.debug(f"Looping for therapist: {therapist_element.text}")
        therapist_element.click()
        return count

    logger.debug("Looping therapists")

    utils.click_element(driver, By.ID, "nav-staff-menu")
    time.sleep(2)
    ul_element = utils.find_element(driver, By.CSS_SELECTOR, "#nav-staff-menu>ul")
    therapist_count = len(ul_element.find_elements(By.CSS_SELECTOR, "li"))
    therapist_iterator = 0
    while therapist_iterator < therapist_count:
        driver.refresh()
        driver.execute_script("window.scrollTo(0, 0);")
        utils.click_element(driver, By.ID, "nav-staff-menu")
        time.sleep(3)
        ul_element = utils.find_element(driver, By.CSS_SELECTOR, "#nav-staff-menu>ul")
        new_count = _helper(driver, therapist_iterator)
        if new_count == therapist_iterator + 1:
            therapist_iterator += 1
            continue
        export_data(driver)
        therapist_iterator += 1


def main():
    login_ta(driver, actions, services, admin=True)
    open_profile(driver)
    loop_therapists(driver)
    time.sleep(60)


if __name__ == "__main__":
    services, config = utils.load_config()
    driver, actions = utils.initialize_selenium(save_profile=True)
    main()
