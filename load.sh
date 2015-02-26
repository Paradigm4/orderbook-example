# usage:
# ./load.sh <Compressed FILE NAME>
#
# example:
# wget  ftp://ftp.nyxdata.com/Historical%20Data%20Samples/TAQ%20NYSE%20ArcaBook/EQY_US_ALL_ARCA_BOOK_20130404.csv.gz
#./load_arca  EQY_US_ALL_ARCA_BOOK_20130404.csv.gz

# Load raw data files...
zcat $1 | grep "^A" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string>[i=0:*,10000,0]" -a load_a -x -t CNNCCNSNNNCS
zcat $1 | grep "^M" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,size:int64,price:double,seconds:int64,milliseconds:int64,symbol:string,exchange:char,system:char,qid:string,ordertype:char>[i=0:*,10000,0]" -a load_m -x
zcat $1 | grep "^D" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,seconds:int64,milliseconds:int64,symbol:string,exchange:char,system:char,qid:string,ordertype:char>[i=0:*,10000,0]" -a load_d -x


# Now combine these three arrays into one called 'flat':

iquery -naq "remove(flat)" 2>/dev/null
iquery -naq "create array flat  <type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [i=0:*,10000,0]"
time iquery -naq "insert(load_a, flat)"
N=$(iquery -ocsv -aq "aggregate(flat, count(*))" | tail -n 1)
echo "Count of add orders is ${N}"
time iquery -naq "insert(cast(redimension(apply(load_m,j,i+${N}), <type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [j=0:*,10000,0]),<type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [i=0:*,10000,0]), flat)"
N=$(iquery -ocsv -aq "aggregate(flat, count(*))" | tail -n 1)
echo "Count of add + modify orders is ${N}"
time iquery -naq "insert(cast(redimension(apply(load_d,j,i+${N},price,double(0), size, int64(0)), <type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [j=0:*,10000,0]),<type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [i=0:*,10000,0]), flat)"
N=$(iquery -ocsv -aq "aggregate(flat, count(*))" | tail -n 1)
echo "Count of add + modify + delete orders is ${N}"

iquery -aq "remove(load_a)"
iquery -aq "remove(load_d)"
iquery -aq "remove(load_m)"


echo "Done loading raw data. Proceed to the ./redim.sh script"
