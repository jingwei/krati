/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
