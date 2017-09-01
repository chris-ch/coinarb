#!/usr/bin/env bash
PYTHONPATH=src python scripts/pricing-source.py --bitfinex btcusd,eosbtc,eosusd | PYTHONPATH=src python scripts/scan-arb.py --strategy eos/usd,eos/btc,btc/usd
#PYTHONPATH=src python scripts/pricing-source.py --bitfinex btcusd,eosbtc,eosusd | PYTHONPATH=src python scripts/echo.py
