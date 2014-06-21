#!/bin/bash
# Example 3-level order book
# orderbook is a custom SciDB aggregation function. It consumes a special string, and it
# produces a comma-separated string formatted like:
# bid price, bid vol, ...,  ask price, ask vol, ...

iquery -aq "load_library('orderbook')"
q="store(variable_window(symbol_time, ms, 1, 0, orderbook(order_record)), book)"
iquery -naq "$q"
