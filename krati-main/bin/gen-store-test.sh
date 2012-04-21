#!/bin/sh

PROG=$0

TEST_NAME=$1
NUM_READERS=$2
HIT_PERCENT=$3
RUN_TIME_SECONDS=$4

if [ $# -lt 4 ]; then
  echo "NAME"
  echo "  $PROG"
  echo "SYNOPSIS"
  echo "  $PROG testName numReaders hitPercent runTimeSeconds"
  echo "EXAMPLES"

  for t in TestDataPartition TestDataPartitionMapped TestDataPartitionChannel TestIndexedStore TestIndexedStoreMapped TestIndexedStoreWriteBuffer TestStaticStore TestStaticStoreMapped TestDynamicStore TestDynamicStoreMapped TestBdbBytes
  do
    echo "  $PROG $t 4 10 600"
  done

  exit 1
fi

LOGS_DIR="target/$TEST_NAME-$HIT_PERCENT"

if [ ! -d $LOGS_DIR ]; then
  mkdir -p $LOGS_DIR
fi

TEST_DATA_PARTITION=`echo $TEST_NAME | sed -e "s/Partition.*/Partition/"`

for i in 01 02 03 04 05 06 07 08 09 10 20 30 40 50 60 70 80 90 100
do
  KEY_COUNT=`expr $i \* 1000000`
  
  if [ $TEST_DATA_PARTITION = "TestDataPartition" ]; then
     ID_COUNT_PARAM="-Dkrati.test.idCount="$KEY_COUNT
  fi

  echo mvn test -Dtest=$TEST_NAME -Dkrati.test.jvm.args=\"-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails\" -Dkrati.test.keyCount=$KEY_COUNT -Dkrati.test.numReaders=$NUM_READERS -Dkrati.test.hitPercent=$HIT_PERCENT -Dkrati.test.runTimeSeconds=$RUN_TIME_SECONDS $ID_COUNT_PARAM
  echo mv target/logs $LOGS_DIR/logs."$i"M
  echo ""
done

