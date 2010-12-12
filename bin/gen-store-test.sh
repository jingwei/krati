#!/bin/sh

PROG=$0

TEST_NAME=$1
NUM_READERS=$2
ACCESS_PERCENT=$3

if [ $# -lt 3 ]; then
  echo "NAME"
  echo "  $PROG"
  echo "SYNOPSIS"
  echo "  $PROG testName numReaders hitPercent"
  echo "EXAMPLES"
  for t in TestBdbBytes TestIndexedStore TestIndexedStoreMapped TestIndexedStoreWriteBuffer TestStaticStore TestStaticStoreMapped TestDynamicStore TestDynamicStoreMapped
  do
    echo "  $PROG $t 4 10"
  done

  exit 1
fi

for i in 01 02 03 04 05 06 07 08 09 10 20 30 40 50 60 70 80 90 100
do
  KEY_COUNT=`expr $i \* 1000000`
  
  if [ $i -le 4 ]; then
     INIT_LEVEL=7
  else
     if [ $i -le 10 ]; then
       INIT_LEVEL=8
     else
       if [ $i -le 40 ]; then
         INIT_LEVEL=10
       else
         INIT_LEVEL=11
       fi
     fi
  fi
  
  echo ant test.clean
  echo ant test.loggc -Dtests.to.run=$TEST_NAME -Dtest.loggc.keyCount=$KEY_COUNT -Dtest.loggc.numReaders=$NUM_READERS -Dtest.loggc.hitPercent=$ACCESS_PERCENT -Dtest.loggc.initLevel=$INIT_LEVEL
  echo mv target/logs logs."$i"M
  echo ""
done

