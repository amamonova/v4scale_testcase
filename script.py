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


def clean_price_target(inp_str):
    """
    This function preprocess raw price string to float number.

    :param inp_str: The string with $ sign and/or with '➝' symbol
    :return: float number of price
    """
    if '➝' in inp_str and len(inp_str) > 1:
        return float(inp_str.split('➝')[1].strip()[1:])
    elif inp_str == '➝':
        return 0.0
    else:
        return float(inp_str[1:])


def preprocess_rating_column(inp_str):
    if type(inp_str) == str and '➝' in inp_str:
        return inp_str.split('➝')[1].strip()
    return inp_str


def get_metrics(dataframe, metric_type, ratings='Sell Hold Buy', freq='D'):
    res = {}
    for t in ratings.split(' '):

        if metric_type == 'ins':
            series = dataframe[dataframe.Rating == t].set_index('Date')\
                .groupby(pd.Grouper(freq=freq))['Price Target'].count()
        elif metric_type == 'amt':
            series = dataframe[dataframe.Rating == t].set_index('Date') \
                .groupby(pd.Grouper(freq=freq))['Price Target'].mean()
        else:
            print('Not implemented type of metric')
            return 0

        # series.index = series.index.map(lambda x: f'{x.year}-{x.month}')
        res[f'{metric_type.upper()}_ANALYST_{t.upper()}'] = series
    return res


def computing(url):

    # getting the table from web page
    soup = get_soup(get_html(url))
    df = pd.read_html(
        str(soup.find('table', {'class': 'scroll-table sort-table'})))[0]

    # table preprocessing: Date column to datetime format,
    # empty fields in Price Target column to 00 string,
    # formatting of Price Target string
    df.Date = df['Date'].astype('datetime64')
    df.Rating = df.Rating.apply(preprocess_rating_column)
    df['Price Target'].fillna('00', inplace=True)
    df['Price Target'] = df['Price Target'].apply(clean_price_target)

    return {'ins': get_metrics(df, 'ins'),
            'amt': get_metrics(df, 'amt')}


def pretty_print(result_dict):
    print('INS METRICS:')
    with pd.option_context('display.max_rows', None):
        ins = pd.concat(result_dict['ins'], axis=1)
        print(ins)

    print('AMT METRICS:')
    with pd.option_context('display.max_rows', None):
        amt = pd.concat(result_dict['amt'], axis=1)
        print(amt)


if __name__ == '__main__':
    url = ('https://www.marketbeat.com/stocks/'
           'NASDAQ/MSFT/price-target/?MostRecent=0')

    res = computing(url)
    pretty_print(res)


