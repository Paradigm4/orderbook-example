#!/bin/bash
# Example 3-level order book
# orderbook is a custom SciDB aggregation function. It consumes a special string, and it
# produces a comma-separated string formatted like:
# bid price, bid vol, ...,  ask price, ask vol, ...

iquery -aq "load_library('orderbook')" || echo "Error: please install the orderbook aggregate. See https://github.com/Paradigm4/orderbook-example"
q="store(variable_window(symbol_time, ms, 1, 0, orderbook(order_record)), book)"
iquery -naq "$q"

echo
echo "Done creating an example book array. Here is its schema:"
iquery -aq "show(book)"

echo
echo "The book array contains the following number of records:"
iquery -aq "aggregate(book, count(*))"
