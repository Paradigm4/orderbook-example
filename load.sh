# usage:
# ./load_arca <FILE NAME>
#
# example:
#./load_arca  EQY_US_ALL_ARCA_BOOK_20130404.csv.gz

# Load raw data files...
zcat $1 | grep "^A" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string>[i=0:*,10000,0]" -a arcaload_a -x -t CNNCCNSNNNCS
zcat $1 | grep "^M" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,size:int64,price:double,seconds:int64,milliseconds:int64,symbol:string,exchange:char,system:char,qid:string,ordertype:char>[i=0:*,10000,0]" -a arcaload_m -x
zcat $1 | grep "^D" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,seconds:int64,milliseconds:int64,symbol:string,exchange:char,system:char,qid:string,ordertype:char>[i=0:*,10000,0]" -a arcaload_d -x


# Now combine these three arrays into one called 'arca_flat':

iquery -naq "remove(arca_flat)"
iquery -naq "create_array(arca_flat, <type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [i=0:*,10000,0])"
time iquery -naq "insert(arcaload_a, arca_flat)"
N=$(iquery -ocsv -aq "aggregate(arca_flat, count(*))" | tail -n 1)
echo "Count of add orders is ${N}"
time iquery -naq "insert(cast(redimension(apply(arcaload_m,j,i+${N}), <type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [j=0:*,10000,0]),<type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [i=0:*,10000,0]), arca_flat)"
N=$(iquery -ocsv -aq "aggregate(arca_flat, count(*))" | tail -n 1)
echo "Count of add + modify orders is ${N}"
time iquery -naq "insert(cast(redimension(apply(arcaload_d,j,i+${N},price,double(0), size, int64(0)), <type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [j=0:*,10000,0]),<type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [i=0:*,10000,0]), arca_flat)"
N=$(iquery -ocsv -aq "aggregate(arca_flat, count(*))" | tail -n 1)
echo "Count of add + modify + delete orders is ${N}"

iquery -aq "remove(arcaload_a)"
iquery -aq "remove(arcaload_d)"
iquery -aq "remove(arcaload_m)"
