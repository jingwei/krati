package test.bdb;

import test.driver.StoreWriter;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;

/**
 * BdbBytesWriter
 * 
 * @author jwu
 * 
 */
public class BdbBytesWriter implements StoreWriter<Database, byte[], byte[]> {
    @Override
    public void put(Database db, byte[] key, byte[] value) throws Exception {
        try {
            DatabaseEntry dbKey = new DatabaseEntry(key);
            DatabaseEntry dbValue = new DatabaseEntry(value);

            // auto-commit put
            db.put(null, dbKey, dbValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
