import glob
import shutil
import time
from typing import Callable

import pandas as pd
from loguru import logger
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver

import shared_utils as utils
from qsend import login_ta


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
                1,
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
                    1,
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


def download_data(driver: WebDriver):
    def _helper(driver: WebDriver, data_title: str):
        logger.debug(f"Downloading {data_title}")
        try:
            utils.click_element(
                driver,
                By.XPATH,
                f"//h5[contains(normalize-space(text()), '{data_title}')]/following-sibling::p/a[contains(text(), 'Download')]",
                1,
            )
            return True
        except NoSuchElementException:
            logger.error(f"Could not find {data_title} Download button")
            return False

    driver.get(driver.current_url + "#therapist-data-export")

    started = _helper(driver, "Client Appointments")
    if not started:
        return
    time.sleep(2)
    _helper(driver, "Clients")
    time.sleep(2)
    _helper(driver, "Insurance Policies and Benefits")
    time.sleep(2)


def loop_therapists(driver: WebDriver, func: Callable):
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

    driver.refresh()
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(2)
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
        func(driver)
        therapist_iterator += 1


def combine_files():
    def read_and_concat_files(pattern, output_file):
        files = glob.glob(pattern)
        df_list = []
        for file in files:
            try:
                df_list.append(pd.read_csv(file, encoding="utf-8"))
            except UnicodeDecodeError:
                df_list.append(pd.read_csv(file, encoding="latin1"))
        df = pd.concat(df_list)
        df.to_csv(output_file, index=False, encoding="utf-8")

    read_and_concat_files(
        "put/downloads/dataExport-appointments*.csv", "put/appointments.csv"
    )
    read_and_concat_files(
        "put/downloads/dataExport-demographic*.csv", "put/demographic.csv"
    )
    read_and_concat_files(
        "put/downloads/dataExport-insurance*.csv", "put/insurance.csv"
    )


def main():
    shutil.rmtree("put/downloads", ignore_errors=True)
    login_ta(driver, actions, services, admin=True)
    open_profile(driver)
    loop_therapists(driver, export_data)
    loop_therapists(driver, download_data)
    combine_files()


if __name__ == "__main__":
    services, config = utils.load_config()
    driver, actions = utils.initialize_selenium(save_profile=True)
    main()
