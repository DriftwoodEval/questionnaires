import shared_utils as utils
from qsend import login_ta


def main():
    login_ta(driver, actions, services)


if __name__ == "__main__":
    services, config = utils.load_config()
    driver, actions = utils.initialize_selenium()
    main()
