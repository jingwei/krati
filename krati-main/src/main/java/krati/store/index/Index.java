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

package krati.store.index;

import java.io.IOException;
import java.util.Map.Entry;

import krati.PersistableListener;
import krati.io.Closeable;
import krati.util.IndexedIterator;

/**
 * Index.
 * 
 * @author jwu
 * 
 * <p>
 * 06/04, 2011 - Added interface Closeable 
 */
public interface Index extends Iterable<Entry<byte[], byte[]>>, Closeable {
    
    /**
     * Gets the capacity of Index.
     */
    public int capacity();
    
    /**
     * Looks up the meta bytes associated with the specified key bytes.
     * 
     * @param keyBytes
     */
    public byte[] lookup(byte[] keyBytes);
    
    /**
     * Updates the mapping from the specified <code>keyBytes</code> to <code>metaBytes</code>.
     * 
     * @param keyBytes
     * @param metaBytes
     * @throws Exception
     */
    public void update(byte[] keyBytes, byte[] metaBytes) throws Exception;
    
    /**
     * Gets the iterator of keys.
     */
    public IndexedIterator<byte[]> keyIterator();
    
    /**
     * Gets the iterator of mappings from key bytes to meta bytes.
     */
    public IndexedIterator<Entry<byte[], byte[]>> iterator();
    
    /**
     * Persist updates to this Index.
     * 
     * @throws IOException
     */
    public void persist() throws IOException;
    
    /**
     * Sync updates to this Index.
     * 
     * @throws IOException
     */
    public void sync() throws IOException;
    
    /**
     * Clears the entire Index.
     * 
     * @throws IOException
     */
    public void clear() throws IOException;
    
    /**
     * Gets the persistable listener.
     */
    public PersistableListener getPersistableListener();
    
    /**
     * Sets the persistable listener.
     */
    public void setPersistableListener(PersistableListener listener);
}
