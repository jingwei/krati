package krati.core.array.entry;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 * EntryPool
 * 
 * @author jwu
 * 
 */
public class EntryPool<T extends EntryValue> {
    private static final Logger _log = Logger.getLogger(EntryPool.class);
    
    private int _entryServiceIdCounter = 0;
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
            _log.info("Entry " + freeEntry.getId() + " created: " + freeEntry.getClass().getSimpleName());
        }
        
        _log.info("Entry " + freeEntry.getId() + " serviceId " + _entryServiceIdCounter);
        freeEntry.setServiceId(_entryServiceIdCounter++);
        return freeEntry;
    }
    
    public void clear() {
        while (!_serviceQueue.isEmpty()) {
            Entry<T> entry = _serviceQueue.poll();
            if (entry != null) addToRecycleQueue(entry);
        }
    }
}
