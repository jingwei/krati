/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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
