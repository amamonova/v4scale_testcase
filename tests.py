import unittest
from script import extract, transform


class TestScript(unittest.TestCase):
    def test_computing(self):
        url = ('https://www.marketbeat.com/stocks/'
               'NASDAQ/MSFT/price-target/?MostRecent=0')
        soup = extract(url)
        result = transform(soup)

        amt_buy_18_10_19 = 105
        self.assertEqual(result['amt']['AMT_ANALYST_BUY']['2019-10-18'],
                         amt_buy_18_10_19)

        amt_buy_19_10_7 = 160
        self.assertEqual(result['amt']['AMT_ANALYST_BUY']['2019-10-07'],
                         amt_buy_19_10_7)

        amt_sell_19_07_19 = 93
        self.assertEqual(result['amt']['AMT_ANALYST_SELL']['2019-07-19'],
                         amt_sell_19_07_19)

        amt_hold_19_07_15 = 147
        self.assertEqual(result['amt']['AMT_ANALYST_HOLD']['2019-07-15'],
                         amt_hold_19_07_15)

        ins_buy_20_2_27 = 4
        self.assertEqual(result['ins']['INS_ANALYST_BUY']['2020-02-27'],
                         ins_buy_20_2_27)

        ins_sell_18_6_11 = 1
        self.assertEqual(result['ins']['INS_ANALYST_SELL']['2018-06-11'],
                         ins_sell_18_6_11)

        ins_hold_19_7_15 = 1
        self.assertEqual(result['ins']['INS_ANALYST_HOLD']['2019-07-15'],
                         ins_hold_19_7_15)