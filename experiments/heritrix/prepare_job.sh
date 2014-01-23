USER='william'
HOST='localhost'
JOBS_DIR='/Users/william/Desktop/test_data/heritrix/jobs/'
JOB='columbia-2-columbia-2.github.io'
DATA_DIR='/Users/william/Desktop/test_data/wmayor/data/columbia-2-columbia-2'

mkdir -p $DATA_DIR
rm $DATA_DIR/*

for f in `ssh $USER@$HOST "find $JOBS_DIR$JOB -name "*.warc.gz" -not -path "*latest*""`; do
    scp $USER@$HOST:$f $DATA_DIR
done

for f in `find $DATA_DIR -name "*.warc.gz"`; do
    gunzip $f
done

du -h $DATA_DIR