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

package krati.core.array.entry;

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueIntFactory.
 * 
 * @author jwu
 */
public class EntryValueIntFactory implements EntryValueFactory<EntryValueInt> {
    /**
     * Creates an array of EntryValueInt of a specified length.
     * 
     * @param length
     *            the length of array
     * @return an array of EntryValueInt(s).
     */
    @Override
    public EntryValueInt[] newValueArray(int length) {
        return new EntryValueInt[length];
    }
    
    /**
     * @return an empty EntryValueInt.
     */
    public EntryValueInt newValue() {
        return new EntryValueInt(0, 0, 0);
    }
    
    /**
     * @return an EntryValueInt read from an input stream.
     * @throws IOException
     */
    @Override
    public EntryValueInt newValue(DataReader in) throws IOException {
        return new EntryValueInt(in.readInt(), /* array position */
                                 in.readInt(), /* data value */
                                 in.readLong() /* SCN value */);
    }
    
    /**
     * Read data from stream to populate an EntryValueInt.
     * 
     * @param in
     *            data reader for EntryValueInt.
     * @param value
     *            an EntryValueInt to populate.
     * @throws IOException
     */
    @Override
    public void reinitValue(DataReader in, EntryValueInt value) throws IOException {
        value.reinit(in.readInt(), /* array position */
                     in.readInt(), /* data value */
                     in.readLong() /* SCN value */);
    }
}
