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

package krati.core.array.entry;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Redo Entry.
 * 
 * @author jwu
 * 
 */
public interface Entry<T extends EntryValue> extends Comparable<Entry<T>> {
    public static final long STORAGE_VERSION = 0;
    
    /**
     * @return the minimum SCN of updates maintained by this Entry.
     */
    public long getMinScn();

    /**
     * @return the maximum SCN of updates maintained by this Entry.
     */
    public long getMaxScn();

    /**
     * Gets the Entry file.
     * 
     * @return this Entry's file.
     */
    public File getFile();

    /**
     * @return the capacity of this Entry.
     */
    public int capacity();

    /**
     * @return the number of EntryValue(s) in this Entry.
     */
    public int size();

    /**
     * @return <code>true</code> if this Entry if filled up. Otherwise,
     *         <code>false</code>
     */
    public boolean isFull();

    /**
     * @return <code>true</code> if this Entry if empty. Otherwise,
     *         <code>false</code>
     */
    public boolean isEmpty();

    /**
     * Compares this Entry to another Entry for sorting purposes.
     */
    public int compareTo(Entry<T> e);

    /**
     * @return a list of EntryValue(s) contained in this Entry.
     */
    public List<T> getValueList();

    /**
     * @return the EntryValue factory.
     */
    public EntryValueFactory<T> getValueFactory();

    /**
     * Saves this Entry to a file.
     * 
     * @param file
     * @throws IOException
     */
    public void save(File file) throws IOException;

    /**
     * Loads an entry from a given file.
     * 
     * @param file
     * @throws IOException
     */
    public void load(File file) throws IOException;

    /**
     * Adds an EntryValue.
     */
    public void add(T value);

    /**
     * Clears this Entry.
     */
    public void clear();
}
