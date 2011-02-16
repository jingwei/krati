package krati.sos;

import java.io.IOException;

/**
 * ObjectCacheAgent:
 * 
 * An agent that wraps an ObjectCache can have inbound and outbound ObjectHandler(s).
 * The inbound handler is associated with the set method. It is called on an inbound object before the object is passed down to the underlying ObjectCache.
 * The outbound handler is associated with the get method. It is called on an outbound object before the object is returned back to the ObjectCache visitor.
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
 * @param <T> Object to be cached.
 */
public class ObjectCacheAgent<T> implements ObjectCache<T>
{
    protected ObjectCache<T> _cache;
    protected ObjectHandler<T> _inboundHandler;
    protected ObjectHandler<T> _outboundHandler;
    
    public ObjectCacheAgent(ObjectCache<T> cache,
                            ObjectHandler<T> inboundHandler,
                            ObjectHandler<T> outboundHandler)
    {
        this._cache = cache;
        this._inboundHandler = inboundHandler;
        this._outboundHandler = outboundHandler;
    }
    
    public ObjectCache<T> getObjectCache()
    {
        return _cache;
    }
    
    public ObjectHandler<T> getInboundHandler()
    {
        return _inboundHandler;
    }
    
    public ObjectHandler<T> getOutboundHandler()
    {
        return _outboundHandler;
    }
    
    @Override
    public int getObjectIdCount()
    {
        return _cache.getObjectIdCount();
    }
    
    @Override
    public int getObjectIdStart()
    {
        return _cache.getObjectIdStart();
    }
    
    @Override
    public boolean delete(int objectId, long scn) throws Exception
    {
        synchronized(_cache)
        {
            return _cache.delete(objectId, scn);
        }
    }
    
    @Override
    public boolean set(int objectId, T object, long scn) throws Exception
    {
        if(object != null && _inboundHandler != null && _inboundHandler.getEnabled())
        {
            _inboundHandler.process(object);
        }
        
        synchronized(_cache)
        {
            return _cache.set(objectId, object, scn);
        }
    }
    
    @Override
    public T get(int objectId)
    {
        T object = _cache.get(objectId);
        if(object != null && _outboundHandler != null && _outboundHandler.getEnabled())
        {
            _outboundHandler.process(object);
        }
        return object;
    }
    
    @Override
    public void sync() throws IOException
    {
        synchronized(_cache)
        {
            _cache.sync();
        }
    }
    
    @Override
    public void persist() throws IOException
    {
        synchronized(_cache)
        {
            _cache.persist();
        }
    }
    
    @Override
    public void clear()
    {
        synchronized(_cache)
        {
            _cache.clear();
        }
    }
    
    @Override
    public long getHWMark()
    {
        return _cache.getHWMark();
    }
    
    @Override
    public long getLWMark()
    {
        return _cache.getLWMark();
    }
    
    @Override
    public void saveHWMark(long endOfPeriod) throws Exception
    {
        _cache.saveHWMark(endOfPeriod);
    }
    
    @Override
    public byte[] getBytes(int objectId)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void expandCapacity(int index) throws Exception {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean hasIndex(int index) {
        return _cache.hasIndex(index);
    }
    
    @Override
    public int length() {
        return _cache.getObjectIdCount();
    }
}
