package krati.sos;

import java.io.IOException;

/**
 * ObjectPartitionAgent:
 * 
 * An agent that wraps an ObjectPartition can have inbound and outbound ObjectHandler(s).
 * The inbound handler is associated with the set method. It is called on an inbound object before the object is passed down to the underlying ObjectPartition.
 * The outbound handler is associated with the get method. It is called on an outbound object before the object is returned back to the ObjectPartition visitor.
 * Either inbound or outbound handlers does not affect the delete method.
 * 
 * <pre>
 *    get(int objectId)
 *      + get object from the underlying store
 *      + Call the outbound handler on the object
 *      + return the object
 *  
 *    set(int objectId, T object, long scn)
 *      + Call the inbound handler on the value object
 *      + delegate operation set to the underlying store
 * 
 * </pre>
 * 
 * @author jwu
 *
 * @param <T> Object to be stored.
 * 
 * <p>
 * 06/04, 2011 - Added support for Closeable
 */
public class ObjectPartitionAgent<T> implements ObjectPartition<T> {
    protected ObjectPartition<T> _partition;
    protected ObjectHandler<T> _inboundHandler;
    protected ObjectHandler<T> _outboundHandler;
    
    public ObjectPartitionAgent(ObjectPartition<T> partition,
                                ObjectHandler<T> inboundHandler,
                                ObjectHandler<T> outboundHandler) {
        this._partition = partition;
        this._inboundHandler = inboundHandler;
        this._outboundHandler = outboundHandler;
    }
    
    public ObjectPartition<T> getObjectPartition() {
        return _partition;
    }
    
    public ObjectHandler<T> getInboundHandler() {
        return _inboundHandler;
    }
    
    public ObjectHandler<T> getOutboundHandler() {
        return _outboundHandler;
    }
    
    @Override
    public int getObjectIdCount() {
        return _partition.getObjectIdCount();
    }
    
    @Override
    public int getObjectIdStart() {
        return _partition.getObjectIdStart();
    }
    
    @Override
    public boolean delete(int objectId, long scn) throws Exception {
        return _partition.delete(objectId, scn);
    }
    
    @Override
    public boolean set(int objectId, T object, long scn) throws Exception {
        if (object != null && _inboundHandler != null && _inboundHandler.getEnabled()) {
            _inboundHandler.process(object);
        }
        
        return _partition.set(objectId, object, scn);
    }
    
    @Override
    public T get(int objectId) {
        T object = _partition.get(objectId);
        if (object != null && _outboundHandler != null && _outboundHandler.getEnabled()) {
            _outboundHandler.process(object);
        }
        return object;
    }
    
    @Override
    public void sync() throws IOException {
        _partition.sync();
    }
    
    @Override
    public void persist() throws IOException {
        _partition.persist();
    }
    
    @Override
    public void clear() {
        _partition.clear();
    }
    
    @Override
    public long getHWMark() {
        return _partition.getHWMark();
    }
    
    @Override
    public long getLWMark() {
        return _partition.getLWMark();
    }
    
    @Override
    public void saveHWMark(long endOfPeriod) throws Exception {
        _partition.saveHWMark(endOfPeriod);
    }
    
    @Override
    public byte[] getBytes(int objectId) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void expandCapacity(int index) throws Exception {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean hasIndex(int index) {
        return _partition.hasIndex(index);
    }
    
    @Override
    public int length() {
        return _partition.getObjectIdCount();
    }
    
    @Override
    public boolean isOpen() {
        return _partition.isOpen();
    }
    
    @Override
    public void open() throws IOException {
        _partition.open();
    }
    
    @Override
    public void close() throws IOException {
        _partition.close();
    }
}
