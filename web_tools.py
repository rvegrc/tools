# Start docker container with silenium before run code.

import pandas as pd

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import WebDriverException
import re
import time


def get_chrome_driver(hub_url):
    """Initialize and return a new Chrome WebDriver instance."""
    chrome_options = ChromeOptions()
    chrome_options.page_load_strategy = 'normal'
    
    try:
        driver = webdriver.Remote(command_executor=hub_url, options=chrome_options)
        return driver
    except WebDriverException as e:
        print(f"Error initializing WebDriver: {e}")
        return None

def check_and_renew_driver(driver: webdriver, hub_url: str) -> webdriver:
    """Check if the driver is alive and renew if necessary."""
    try:
        driver.title  # Attempting to access a property to check if it's still active
        return driver  # Driver is still active
    except (WebDriverException, AttributeError):
        print("WebDriver is not active. Reinitializing...")
        return get_chrome_driver(hub_url)
    
hub_url = "http://localhost:4444/wd/hub" # docker container with selenium and chrome
# hub_url = "http://chrome:4444/wd/hub" # docker container with spark and chrome. chrome is a service name in docker-compose.yml

# Example usage
# chrome_driver = get_chrome_driver(hub_url)

# # Later in the code, before using the driver:
# chrome_driver = check_and_renew_driver(chrome_driver, hub_url)

def close_driver(driver):
    """Close the WebDriver instance."""
    try:
        driver.quit()
    except WebDriverException as e:
        print(f"Error closing WebDriver: {e}")