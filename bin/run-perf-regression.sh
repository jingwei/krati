mvn clean

mvn test -Dtest=TestDataPartition -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=5000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=600 -Dkrati.test.idCount=5000000

mvn test -Dtest=TestDataPartitionMapped -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=5000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=600 -Dkrati.test.idCount=5000000

mvn test -Dtest=TestDataPartitionChannel -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=5000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=600 -Dkrati.test.idCount=5000000

mvn test -Dtest=TestStaticStore -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=5000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=600

mvn test -Dtest=TestDynamicStore -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=5000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=600

mvn test -Dtest=TestIndexedStore -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=5000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=600

mvn test -Dtest=TestStaticStoreChannel -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=10000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=900

mvn test -Dtest=TestDynamicStoreChannel -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=10000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=900

mvn test -Dtest=TestIndexedStoreChannel -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=10000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=900

mvn test -Dtest=TestIndexedStoreWriteBuffer -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=10000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=3600

mvn test -Dtest=TestIndexedStoreWriteBuffer -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=50000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=3600

mvn test -Dtest=TestIndexedStoreWriteBuffer -Dkrati.test.jvm.args="-Xloggc:target/logs/krati.gc -XX:+PrintGCDetails" -Dkrati.test.keyCount=100000000 -Dkrati.test.numReaders=4 -Dkrati.test.hitPercent=10 -Dkrati.test.runTimeSeconds=3600

mv target/logs run-perf-logs
