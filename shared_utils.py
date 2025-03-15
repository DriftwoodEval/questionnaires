import logging
import os
from datetime import datetime
from time import sleep, strftime, strptime

import yaml
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select


def load_config():
    with open("./config/info.yml", "r") as file:
        logging.info("Loading info file")
        info = yaml.safe_load(file)
        services = info["services"]
        config = info["config"]
        return services, config


def initialize_selenium():
    logging.info("Initializing Selenium")
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    if os.getenv("HEADLESS") == "true":
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    actions = ActionChains(driver)
    driver.implicitly_wait(5)
    driver.set_window_size(1920, 1080)
    return driver, actions


def get_previous_clients():
    logging.info("Loading previous clients")
    filepath = "./put/clients.yml"
    try:
        with open(filepath, "r") as file:
            prev_clients = yaml.safe_load(file)
    except FileNotFoundError:
        logging.info(f"{filepath} does not exist.")
        return None
    return prev_clients


def update_yaml(clients, filepath):
    try:
        with open(filepath, "r") as file:
            current_yaml = yaml.safe_load(file)
    except FileNotFoundError:
        logging.info(f"{filepath} does not exist, creating new file")
        current_yaml = None

    if current_yaml is None:
        logging.info(f"Dumping {clients} to {filepath}")
        with open(filepath, "w") as file:
            yaml.dump(clients, file, default_flow_style=False)
    else:
        logging.info(f"Updating {filepath}")
        current_yaml.update(clients)
        with open(filepath, "w") as file:
            logging.info(f"Dumping {clients} to {filepath}")
            yaml.dump(current_yaml, file, default_flow_style=False)
