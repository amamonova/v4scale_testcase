from bs4 import BeautifulSoup
import pandas as pd
from prefect import task, Flow
import requests


@task
def extract(url):
    """
    This function gets an url parameter and returns html soup
    (a complex tree of Python objects).

    :param url: The url with needed info
    :return: soup
    """
    r = requests.get(url)
    return BeautifulSoup(r.text, 'html.parser')


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
    """
    This function preprocess raw rating string to string with one word.

    :param inp_str: The string with/without '➝' symbol
    :return: clean string
    """
    if isinstance(inp_str, str) == str and '➝' in inp_str:
        return inp_str.split('➝')[1].strip()
    return inp_str


def get_metrics(dataframe, metric_type, ratings='Sell Hold Buy', freq='D'):
    """
    This function computes metrics AMT_ANALYST_BUY, AMT_ANALYST_SELL,
    AMT_ANALYST_HOLD, INS_ANALYST_BUY, INS_ANALYST_SELL, INS_ANALYST_HOLD

    :param dataframe: pandas dataframe
    :param metric_type: string - 'ins' or 'amt' implemented
    :param ratings: string - types of rating sliced by space.
    'Sell Hold Buy' by default
    :param freq: string - 'D' by default. Frequency of grouping
    :return: dict with names of metrics as keys and pd.Series as values
    """
    res = {}
    for t in ratings.split(' '):

        if metric_type == 'ins':
            series = dataframe[dataframe.Rating == t].set_index('Date') \
                .groupby(pd.Grouper(freq=freq))['Price Target']\
                .count().replace(0, pd.NA)
            series = series.astype('Int64')
        elif metric_type == 'amt':
            series = dataframe[dataframe.Rating == t].set_index('Date') \
                .groupby(pd.Grouper(freq=freq))['Price Target']\
                .mean().replace(0, pd.NA).fillna(pd.NA)
        else:
            print('Not implemented type of metric')
            return 0

        res[f'{metric_type.upper()}_ANALYST_{t.upper()}'] = series
    return res


@task
def transform(soup):
    """
    This function transforms the dataframe and computes results

    :param soup: soup
    :return: dict with results
    """

    # getting the table from web page
    df = pd.read_html(
        str(soup.find('table', {'class': 'scroll-table sort-table'})))[0]

    # table pre processing: Date column to datetime format,
    # empty fields in Price Target column to 00 string,
    # formatting of Price Target string
    df.Date = df['Date'].astype('datetime64')
    df.Rating = df.Rating.apply(preprocess_rating_column)
    df['Price Target'].fillna('00', inplace=True)
    df['Price Target'] = df['Price Target'].apply(clean_price_target)

    ins_metrics = get_metrics(df, 'ins')
    amt_metrics = get_metrics(df, 'amt')

    result = []
    for date in df.Date.dt.strftime('%Y-%m-%d').unique():
        result.append({'date': date,
                       'AMT_ANALYST_SELL': amt_metrics['AMT_ANALYST_SELL']
                      .get(date, pd.NA),
                       'AMT_ANALYST_BUY': amt_metrics['AMT_ANALYST_BUY']
                      .get(date, pd.NA),
                       'AMT_ANALYST_HOLD': amt_metrics['AMT_ANALYST_HOLD']
                      .get(date, pd.NA),
                       'INS_ANALYST_SELL': ins_metrics['INS_ANALYST_SELL']
                      .get(date, pd.NA),
                       'INS_ANALYST_BUY': ins_metrics['INS_ANALYST_BUY']
                      .get(date, pd.NA),
                       'INS_ANALYST_HOLD': ins_metrics['INS_ANALYST_HOLD']
                      .get(date, pd.NA)})
    return result


@task
def load(result_dict):
    """
    This function prints results. Two tables INS metrics ans AMT metrics.

    :param result_dict: dict with results
    :return: None
    """
    print(result_dict)


if __name__ == '__main__':
    with Flow('ms-etl') as flow:
        url = ('https://www.marketbeat.com/stocks/'
               'NASDAQ/MSFT/price-target/?MostRecent=0')
        soup = extract(url)
        res = transform(soup)
        load(res)
    flow.run()

