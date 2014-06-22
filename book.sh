#!/bin/bash
# Example 3-level order book
# orderbook is a custom SciDB aggregation function. It consumes a special string, and it
# produces a comma-separated string formatted like:
# bid price, bid vol, ...,  ask price, ask vol, ...

iquery -aq "load_library('orderbook')" || echo "Error: please install the orderbook aggregate. See https://github.com/Paradigm4/orderbook-example"
iquery -naq "remove(book)" 2>/dev/null
# Prepare the book entries and redimension into a more reasonable chunk size
# (the orderbook aggregate has special chunk size requirements)
#
# NOTE! Be careful about chunk size selection in the redimension.
# SciDB wants a chunk size that, on average, has about a million non-empty
# cells per chunk or so. It's better to err on the high side here than
# to create an array with tons of chunks with hardly anything in them.
#
q="store(redimension(variable_window(symbol_time, ms, 1, 0, orderbook(order_record)),<order_record_orderbook:string null> [symbol_index=0:*,100,0,ms=0:86399999,10000000,0]), book)"
iquery -naq "$q"

echo
echo "Done creating an example book array. Here is its schema:"
iquery -aq "show(book)"

echo
echo "The book array contains the following number of records:"
iquery -aq "aggregate(book, count(*))"
