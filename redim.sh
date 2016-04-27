#!/bin/bash

# Redimension into an array with time and symbol coordinate axes.
# First we need to make a (small) auxiliary array of unique symbol names:

# Store into a new array called 'symbols'
iquery -naq "store(uniq(sort(project(flat,symbol))), symbols)"

# This query assigns integer levels to each symbol in a new attribute named 'symbol_index':
# iquery -naq "index_lookup(flat, symbols, symbol, symbol_index)"

# Notes:
# The orderbook aggregate requires a few special things:
# 1) Time must be in a single chunk. Here we aggregate over ms offset from the day.
#   (you can have other dimensions, for example time up to the day. So this would run
#    easily for example on a daily basis. With some coding effort, this restriction
#    can be relaxed.)
# 2) orderbook works on a SciDB string attribute with a very special form:
#    'entry_type,ref_unique_id,price,size,symbol,ordertype|'
#    don't ask...
#
# This query sets up the special string argument for orderbook called 'order_record', and redimensions
# it into a 2-d array by symbol_index and ms.
time iquery -naq "
store(
redimension(
  apply(
    index_lookup(flat as X, symbols as Y, X.symbol, symbol_index),
    order_record, type+','+format(ref,'%.0f')+','+format(price,'%f')+','+format(size, '%.0f')+','+symbol +','+ordertype + '|', ms, seconds*1000 + milliseconds),
  <order_record: string null> [symbol_index=0:*,5,0, ms=0:86399999, 86400000, 0]
,false), symbol_time)"
#Note that the above redimension loses some data due to collisions because of the symbol and time dimensions. 
#This script is built to be a general example that doesn't require tuning. You can add a synthetic dimension to store the collisions. 
#[symbol_index=0:*,5,0, ms=0:86399999, 86400000, 0, synth_dim=0:999,1000,0]

echo
echo "Done redimensioning into array symbol_index."
echo "Ready to run  book.sh"
