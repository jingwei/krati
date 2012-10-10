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

import org.apache.log4j.Logger;

import krati.PersistableListener;
import krati.core.StoreConfig;
import krati.store.DynamicDataStore;
import krati.util.IndexedIterator;

/**
 * HashIndex is for serving index lookup from main memory and has the
 * best performance when {@link krati.core.segment.MemorySegmentFactory MemorySegmentFactory}
 * is used to store indexes in memory.
 * 
 * @author jwu
 * 
 * <p>
 * 06/04, 2011 - Added support for Closeable <br/>
 * 06/28, 2011 - Added constructor using StoreConfig <br/>
 * 08/21, 2011 - code cleanup <br/>
 */
public class HashIndex implements Index {
    private final static Logger _logger = Logger.getLogger(HashIndex.class);
    private final DynamicDataStore _store;
    
    /**
     * Creates a new HashIndex instance.
     * 
     * @param config - HashIndex configuration
     * @throws Exception if the index cannot be created.
     */
    public HashIndex(StoreConfig config) throws Exception {
        _store = new DynamicDataStore(config);
        _logger.info("init " + config.getHomeDir().getPath());
    }
    
    @Override
    public final int capacity() {
        return _store.capacity();
    }
    
    @Override
    public void sync() throws IOException {
        _store.sync();
    }
    
    @Override
    public void persist() throws IOException {
        _store.persist();
    }
    
    @Override
    public void clear() throws IOException {
        _store.clear();
    }
    
    @Override
    public byte[] lookup(byte[] keyBytes) {
        return _store.get(keyBytes);
    }
    
    @Override
    public void update(byte[] keyBytes, byte[] metaBytes) throws Exception {
        _store.put(keyBytes, metaBytes);
    }
    
    @Override
    public IndexedIterator<byte[]> keyIterator() {
        return _store.keyIterator();
    }
    
    @Override
    public IndexedIterator<Entry<byte[], byte[]>> iterator() {
        return _store.iterator();
    }
    
    @Override
    public boolean isOpen() {
        return _store.isOpen();
    }
    
    @Override
    public void open() throws IOException {
        _store.open();
    }
    
    @Override
    public void close() throws IOException {
        _store.close();
    }
    
    /**
     * Gets the persistable event listener.
     */
    public final PersistableListener getPersistableListener() {
        return _store.getPersistableListener();
    }
    
    /**
     * Sets the persistable event listener.
     * 
     * @param listener
     */
    public final void setPersistableListener(PersistableListener listener) {
        _store.setPersistableListener(listener);
    }
}
