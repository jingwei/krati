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

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * EntryPool
 * 
 * @author jwu
 * 
 */
public class EntryPool<T extends EntryValue> {
    private final int _entryCapacity;
    private final EntryFactory<T> _entryFactory;
    private final ConcurrentLinkedQueue<Entry<T>> _serviceQueue;
    private final ConcurrentLinkedQueue<Entry<T>> _recycleQueue;
    
    public EntryPool(EntryFactory<T> factory, int entryCapacity) {
        this._entryFactory = factory;
        this._entryCapacity = entryCapacity;
        this._serviceQueue = new ConcurrentLinkedQueue<Entry<T>>();
        this._recycleQueue = new ConcurrentLinkedQueue<Entry<T>>();
    }
    
    public final int getEntryCapacity() {
        return _entryCapacity;
    }
    
    public final EntryFactory<T> getEntryFactory() {
        return _entryFactory;
    }
    
    public boolean isServiceQueueEmpty() {
        return _serviceQueue.isEmpty();
    }
    
    public boolean isRecycleQueueEmpty() {
        return _recycleQueue.isEmpty();
    }
    
    public Entry<T> pollFromService() {
        return _serviceQueue.poll();
    }
    
    public int getServiceQueueSize() {
        return _serviceQueue.size();
    }
    
    public int getReycleQueueSize() {
        return _recycleQueue.size();
    }
    
    public boolean addToServiceQueue(Entry<T> entry) {
        return _serviceQueue.add(entry);
    }
    
    public boolean addToRecycleQueue(Entry<T> entry) {
        entry.clear();
        return _recycleQueue.add(entry);
    }
    
    public Entry<T> next() {
        Entry<T> freeEntry = _recycleQueue.poll();
        if (freeEntry == null) {
            freeEntry = _entryFactory.newEntry(_entryCapacity);
        }
        
        return freeEntry;
    }
    
    public void clear() {
        while (!_serviceQueue.isEmpty()) {
            Entry<T> entry = _serviceQueue.poll();
            if (entry != null) addToRecycleQueue(entry);
        }
    }
}
