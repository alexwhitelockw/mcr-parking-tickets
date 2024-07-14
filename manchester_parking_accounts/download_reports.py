from bs4 import BeautifulSoup
import json
import logging
import os
import re
import requests
import time


logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s",
    filename="report_downloader.log",
    encoding="utf-8",
    level=logging.INFO)


def load_report_links() -> list | None:
    try:
        with open("data/report_links.txt", "r") as f:
            return f.read().splitlines()
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return None


def save_report_link(report_link: str) -> None:
    try:
        with open("data/report_links.txt", "a") as f:
            logger.info(f"{report_link} saved to file {f}.")
            f.write(f"{report_link}\n")
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return None


def scrape_report_links(url: str) -> list | None:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error fetching the URL {url}: {e}")
        return None

    report_page = BeautifulSoup(response.content, "html.parser")
    report_page_links = report_page.find_all(href=re.compile("https://www.manchester.gov.uk/open/downloads/file/"))
    report_page_links = list(set([link.get("href") for link in report_page_links]))
    logger.info("Report links scraped.")
    return report_page_links


def scrape_report_download_link(url: str) -> str | None:

    try:
        report_links = load_report_links()

        if url in report_links:
            logger.info(f"{url} previously scraped, skipping.")
            return None
    except TypeError:
        logger.info("File not created.")

    try:
        time.sleep(5)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error fetching the URL {url}: {e}")
        return None

    report_page = BeautifulSoup(response.content, "html.parser")
    csv_link = report_page.find(href=re.compile(".csv$"))

    if not csv_link:
        logger.info("No CSV link found.")
        save_report_link(url)
        logger.info(f"{url} saved.")
        return None

    csv_link = csv_link.get("href")
    logger.info(f"{csv_link} successfully scraped.")

    save_report_link(url)
    logger.info(f"{url} saved.")

    return csv_link


def download_csv_file(url: str) -> None:
    try:
        time.sleep(5)
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error fetching the URL {url}: {e}")
        return None

    file_name = os.path.basename(url)
    file_path = f"data/source/{file_name}"

    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    logger.info(f"CSV file content downloaded and saved to {file_path}.")

    return None



if __name__ == "__main__":

    # The approach is that all links are initially scraped. These are iterated
    # over to identify which contains a CSV file link. In either case, the 
    # overarching link is recorded in a TXT file to prevent scraping the links
    # again on each run.

    report_links = scrape_report_links("https://www.manchester.gov.uk/open/downloads/91/parking_account")
    report_files = [scrape_report_download_link(link) for link in report_links]
    [download_csv_file(link) for link in report_files if link is not None]
