#!/usr/bin/env bash
PYTHONPATH=src python scripts/pricing-source.py --bitfinex btcusd,eosbtc,eosusd | PYTHONPATH=src python scripts/scan-arb.py --strategy eos/usd,eos/btc,btc/usd --threshold usd:0.05 --amount 100
