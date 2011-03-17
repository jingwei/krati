package test.bdb;

import test.driver.StoreReader;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

/**
 * BdbBytesReader
 * 
 * @author jwu
 * 
 */
public class BdbBytesReader implements StoreReader<Database, byte[], byte[]> {
    @Override
    public final byte[] get(Database db, byte[] key) {
        DatabaseEntry dbKey = new DatabaseEntry(key);
        DatabaseEntry dbValue = new DatabaseEntry();
        OperationStatus status = db.get(null, dbKey, dbValue, null);
        return (status == OperationStatus.SUCCESS) ? dbValue.getData() : null;
    }
}
