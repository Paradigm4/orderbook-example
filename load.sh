# usage:
# ./load.sh <Compressed FILE NAME>
#
# example:
# wget  ftp://ftp.nyxdata.com/Historical%20Data%20Samples/TAQ%20NYSE%20ArcaBook/EQY_US_ALL_ARCA_BOOK_20130404.csv.gz
#./load_arca  EQY_US_ALL_ARCA_BOOK_20130404.csv.gz

# Load raw data files...
#zcat $1 | grep "^A" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string>[i=0:*,10000,0]" -a load_a -x -t CNNCCNSNNNCS
#zcat $1 | grep "^M" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,size:int64,price:double,seconds:int64,milliseconds:int64,symbol:string,exchange:char,system:char,qid:string,ordertype:char>[i=0:*,10000,0]" -a load_m -x
#zcat $1 | grep "^D" | loadcsv.py  -s "<type:char,sequence:int64,ref:int64,seconds:int64,milliseconds:int64,symbol:string,exchange:char,system:char,qid:string,ordertype:char>[i=0:*,10000,0]" -a load_d -x

rm -f /tmp/pipe1
mkfifo /tmp/pipe1
rm -f /tmp/pipe2
mkfifo /tmp/pipe2
rm -f /tmp/pipe3
mkfifo /tmp/pipe3

zcat $1 | grep "^A" > /tmp/pipe1
zcat $1 | grep "^M" > /tmp/pipe2 
zcat $1 | grep "^D" > /tmp/pipe3

iquery -anq "store(project(unpack(apply(aio_input('/tmp/pipe1','attribute_delimiter=,', 'num_attributes=12'),
type, char(a0),
sequence, dcast(a1,int64(null)),
ref,dcast(a2,int64(null)),
exchange, char(a3),
ordertype,char(a4),
size,dcast(a5, int64(null)),
symbol, string(a6),
price, dcast(a7, double(null)),
seconds, dcast(a8, int64(null)),
milliseconds, dcast(a9, int64(null)),
system, char(a10),
qid, a11
),i,10000),
type,
sequence,
ref,
exchange,
ordertype,
size,
symbol,
price,
seconds,
milliseconds,
system,
qid
), load_a)"


iquery -anq "store(project(unpack(apply(aio_input('/tmp/pipe2', 'attribute_delimiter=,', 'num_attributes=12'),
type, char(a0),
sequence, dcast(a1,int64(null)),
ref,dcast(a2,int64(null)),
size,dcast(a3,int64(null)),
price,dcast(a4,double(null)),
seconds,dcast(a5, int64(null)),
milliseconds,dcast(a6, int64(null)),
symbol, string(a7),
exchange, char(a8),
system, char(a9),
qid, a10,
ordertype, char(a11)
),i,10000),
type,
sequence,
ref,
size,
price,
seconds,
milliseconds,
symbol,
exchange,
system,
qid,
ordertype
), load_m)"


iquery -anq "store(project(unpack(apply(aio_input('/tmp/pipe3','attribute_delimiter=,', 'num_attributes=10'),
type, char(a0),
sequence, dcast(a1,int64(null)),
ref,dcast(a2,int64(null)),
seconds,dcast(a3,int64(null)),
milliseconds,dcast(a4,int64(null)),
symbol,string(a5),
exchange, char(a6),
system, char(a7),
qid, a8,
ordertype, char(a9)
),i,10000),
type,
sequence,
ref,
seconds,
milliseconds,
symbol,
exchange,
system,
qid,
ordertype
), load_d)"



# Now combine these three arrays into one called 'flat':

iquery -naq "remove(flat)" 2>/dev/null
iquery -naq "create array flat  <type:char,sequence:int64,ref:int64,exchange:char,ordertype:char,size:int64,symbol:string,price:double,seconds:int64,milliseconds:int64,system:char,qid:string> [i=0:*,10000,0]"
iquery -aq "show(load_a)"
iquery -aq "show(flat)"
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
