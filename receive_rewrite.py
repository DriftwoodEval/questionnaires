from datetime import date

from dateutil.relativedelta import relativedelta
from loguru import logger
from playwright.sync_api import Page, sync_playwright

from utils.custom_types import (
    AdminEmailInfo,
    Config,
    FailedClientFromDB,
    Services,
    validate_questionnaires,
)
from utils.database import get_previous_clients, update_failure_in_db
from utils.misc import load_config
from utils.openphone import OpenPhone
from utils.questionnaires import check_questionnaires, filter_inactive_and_not_pending
from utils.selenium import (
    check_if_docs_signed,
    check_if_opened_portal,
    go_to_client,
    login_ta,
)


def check_failures(
    config: Config,
    services: Services,
    page: Page,
    failed_clients: dict[int, FailedClientFromDB],
):
    """Checks the failures of clients and updates them in the database."""
    login_ta(page, services)

    two_years_ago = date.today() - relativedelta(years=2)
    five_years_ago = date.today() - relativedelta(years=5)

    for client_id, client in failed_clients.items():
        reason = client.failure["reason"]
        print(reason)
        is_resolved = False

        if reason in ["portal not opened", "docs not signed"]:
            go_to_client(page, str(client_id))
            if reason == "portal not opened":
                is_resolved = check_if_opened_portal(page)
            elif reason == "docs not signed":
                is_resolved = check_if_docs_signed(page)

        elif reason == "too young for asd" and client.dob is not None:
            is_resolved = client.dob < two_years_ago

        elif reason == "too young for adhd" and client.dob is not None:
            is_resolved = client.dob < five_years_ago

        if is_resolved:
            update_failure_in_db(config, client_id, reason, resolved=True)
            logger.info(f"Resolved failure for {client.fullName}")
        else:
            # This updates the checked date
            update_failure_in_db(config, client_id, reason)


def main():
    """Docstring for main."""
    services, config = load_config()
    openphone = OpenPhone(config, services)
    email_info: AdminEmailInfo = {
        "ignoring": [],
        "failed": [],
        "call": [],
        "completed": [],
        "errors": [],
        "ifsp_download_needed": [],
    }

    clients, failed_clients = get_previous_clients(config, True)
    print(failed_clients)
    if clients is None:
        logger.critical("Failed to get previous clients")
        return

    email_info["ifsp_download_needed"] = [
        client
        for _, client in clients.items()
        if client.ifsp and not client.ifspDownloaded
    ]

    clients = validate_questionnaires(clients)
    clients = filter_inactive_and_not_pending(clients)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        page = browser.new_page()
        # email_info["completed"], email_info["errors"] = check_questionnaires(
        #     page, config, clients
        # )

        check_failures(config, services, page, failed_clients)


if __name__ == "__main__":
    main()
