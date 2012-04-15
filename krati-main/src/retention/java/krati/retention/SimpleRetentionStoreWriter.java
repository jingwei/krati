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

package krati.retention;

import java.io.IOException;

import org.apache.log4j.Logger;

import krati.retention.clock.Clock;
import krati.retention.clock.WaterMarksClock;
import krati.store.DataStore;

/**
 * SimpleRetentionStoreWriter
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/16, 2011 - Created <br/>
 * 10/17, 2011 - Fixed getLWMark() <br/>
 * 01/06, 2012 - Initialize high water mark from {@link WaterMarksClock} only <br/>
 */
public class SimpleRetentionStoreWriter<K, V> implements RetentionStoreWriter<K, V> {
    private final static Logger _logger = Logger.getLogger(SimpleRetentionStoreWriter.class);
    private final String _source;
    private final DataStore<K, V> _store;
    private final Retention<K> _retention;
    private final WaterMarksClock _waterMarksClock;
    private volatile long _hwmScn = 0;
    
    /**
     * Creates a new RetentionStoreWriter instance.
     * 
     * @param source    - the source of store
     * @param retention - the retention for store update events.  
     * @param store     - the store
     * @param waterMarksClock
     */
    public SimpleRetentionStoreWriter(String source, Retention<K> retention, DataStore<K, V> store, WaterMarksClock waterMarksClock) {
        this._source = source;
        this._retention = retention;
        this._store = store;
        this._waterMarksClock = waterMarksClock;
        
        // Initialize the high water mark scn
        _hwmScn = waterMarksClock.getHWMScn(source);
        
        // Reset low/high water marks if necessary
        long lwmScn = waterMarksClock.getLWMScn(source);
        if(_hwmScn < lwmScn) {
            lwmScn = _hwmScn;
            waterMarksClock.updateWaterMarks(source, lwmScn, _hwmScn);
        } else {
            waterMarksClock.setHWMark(source, _hwmScn);
        }
        
        // Log water marks
        getLogger().info(String.format("init %s lwmScn=%d hwmScn=%d", source, lwmScn, _hwmScn));
    }
    
    protected Logger getLogger() {
        return _logger;
    }
    
    public final DataStore<K, V> getStore() {
        return _store;
    }
    
    public final Retention<K> getRetention() {
        return _retention;
    }
    
    @Override
    public final String getSource() {
        return _source;
    }
    
    @Override
    public long getLWMark() {
        return _waterMarksClock.getLWMScn(_source);
    }
    
    @Override
    public long getHWMark() {
        return _hwmScn;
    }
    
    @Override
    public synchronized void saveHWMark(long hwMark) {
        if(hwMark > _hwmScn) {
            _hwmScn = hwMark;
            _waterMarksClock.setHWMark(_source, _hwmScn);
        }
    }
    
    @Override
    public synchronized void persist() throws IOException {
        _store.persist();
        _retention.flush();
        _waterMarksClock.setHWMark(_source, _hwmScn);
        _waterMarksClock.syncWaterMarks(_source);
    }
    
    @Override
    public synchronized void sync() throws IOException {
        _store.sync();
        _retention.flush();
        _waterMarksClock.setHWMark(_source, _hwmScn);
        _waterMarksClock.syncWaterMarks(_source);
    }
    
    @Override
    public synchronized boolean put(K key, V value, long scn) throws Exception {
        if(scn >= _hwmScn) {
            Clock clock;
            
            _store.put(key, value);
            clock = (scn == _hwmScn) ?
                    _waterMarksClock.current() :
                    _waterMarksClock.updateHWMark(_source, scn);
            _retention.put(new SimpleEvent<K>(key, clock));
            _hwmScn = scn;
            
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public synchronized boolean delete(K key, long scn) throws Exception {
        if(scn >= _hwmScn) {
            Clock clock;
            
            _store.delete(key);
            clock = (scn == _hwmScn) ?
                    _waterMarksClock.current() :
                    _waterMarksClock.updateHWMark(_source, scn);
            _retention.put(new SimpleEvent<K>(key, clock));
            _hwmScn = scn;
            
            return true;
        } else {
            return false;
        }
    }
}
