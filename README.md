# V4SCALE test case

This repo contains script for parsing MICROSOFT (NASDAQ:MSFT) 
CURRENT ANALYST COVERAGE SUMMARY from 
[MarketBeat](https://www.marketbeat.com/stocks/NASDAQ/MSFT/price-target/?MostRecent=0).

## Getting Started

Clone this repo and start the `script.py` file.

```shell script
python script.py
```

### Prerequisites

Download all requirements:

```shell script
pip install -r requirements.txt 
```  

## Running the tests

Start `tests.py` file for testing the script. If an output is 
clear, the script works without assertion errors. 
Doesn't work with new version of script.

```shell script
python tests.py
```

### Tests

Testing the INS and AMT metrics for different dates. 