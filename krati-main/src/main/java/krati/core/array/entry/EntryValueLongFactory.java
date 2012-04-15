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

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueLongFactory.
 * 
 * @author jwu
 */
public class EntryValueLongFactory implements EntryValueFactory<EntryValueLong> {
    /**
     * Creates an array of EntryValueLong of a specified length.
     * 
     * @param length
     *            the length of array
     * @return an array of EntryValueLong(s).
     */
    public EntryValueLong[] newValueArray(int length) {
        return new EntryValueLong[length];
    }
    
    /**
     * @return an empty EntryValueLong.
     */
    public EntryValueLong newValue() {
        return new EntryValueLong(0, 0L, 0L);
    }
    
    /**
     * @return an EntryValueLong read from an input stream.
     * @throws IOException
     */
    public EntryValueLong newValue(DataReader in) throws IOException {
        return new EntryValueLong(in.readInt(),  /* array position */
                                  in.readLong(), /* data value */
                                  in.readLong()  /* SCN value */);
    }
    
    /**
     * Read data from stream to populate an EntryValueLong.
     * 
     * @param in
     *            data reader for EntryValueLong.
     * @param value
     *            an EntryValue to populate.
     * @throws IOException
     */
    @Override
    public void reinitValue(DataReader in, EntryValueLong value) throws IOException {
        value.reinit(in.readInt(),  /* array position */
                     in.readLong(), /* data value */
                     in.readLong()  /* SCN value */);
    }
}
