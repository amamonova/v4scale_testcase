from bs4 import BeautifulSoup
import pandas as pd
import requests


def get_html(url):
    """
    This function gets an url parameter and return string with response.

    :param url: The url with needed info
    :return: string with HTML
    """
    r = requests.get(url)
    return r.text


def get_soup(html):
    """
    This function return parsed objects of given HTML page.

    :param html: string with HTML
    :return: soup (a complex tree of Python objects)
    """
    return BeautifulSoup(html, 'html.parser')


