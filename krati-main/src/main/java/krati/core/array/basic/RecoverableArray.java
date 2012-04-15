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

package krati.core.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import krati.Persistable;
import krati.array.Array;
import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryFactory;
import krati.core.array.entry.EntryValue;

/**
 * RecoverableArray
 * 
 * @author jwu
 * 
 */
public interface RecoverableArray<V extends EntryValue> extends Array, Persistable {
    
    /**
     * Gets the home directory of this array.
     */
    public File getDirectory();
    
    /**
     * Gets the factory for creating new redo log entries.
     */
    public EntryFactory<V> getEntryFactory();
    
    /**
     * Updates the underlying array file with the specified list of redo log entries.
     * 
     * @param entryList - the list of redo log entries.
     * @throws IOException if any redo log entry in the list can not be applied.
     */
    public void updateArrayFile(List<Entry<V>> entryList) throws IOException;
}
