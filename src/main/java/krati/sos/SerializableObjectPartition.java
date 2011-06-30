package krati.sos;

import java.io.IOException;

import krati.array.DynamicArray;
import krati.io.Serializer;
import krati.store.ArrayStorePartition;

/**
 * A simple data partition for serializable objects.
 * 
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of SerializableObjectPartition for a given data parition.
 *    2. There is one and only one thread is calling set and delete methods at any given time. 
 * </pre>
 * 
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 * 
 * @param <T> Serializable object.
 */
public class SerializableObjectPartition<T> implements ObjectPartition<T> {
    protected final ArrayStorePartition _partition;
    protected final Serializer<T> _serializer;

    /**
     * Constructs an array-like object partition for serializable objects.
     * 
     * @param partition
     *            the underlying data partition to store serializable objects.
     * @param serializer
     *            the object serializer to serialize/de-serialize objects.
     */
    public SerializableObjectPartition(ArrayStorePartition partition, Serializer<T> serializer) {
        this._partition = partition;
        this._serializer = serializer;
    }

    /**
     * @return the underlying data partition.
     */
    protected ArrayStorePartition getContentPartition() {
        return _partition;
    }

    /**
     * @return the object serializer.
     */
    public Serializer<T> getSerializer() {
        return _serializer;
    }

    /**
     * @return the total number of objects in the partition.
     */
    @Override
    public int getObjectIdCount() {
        return _partition.getIdCount();
    }

    /**
     * @return the start of ObjectId(s) allowed by the partition.
     */
    @Override
    public int getObjectIdStart() {
        return _partition.getIdStart();
    }

    /**
     * Gets an object based on a user-specified object Id.
     * 
     * @param objectId
     *            the Id of an object to be retrieved from the partition.
     * @return an object associated with the given objectId.
     */
    @Override
    public T get(int objectId) {
        return getSerializer().deserialize(_partition.get(objectId));
    }

    /**
     * Sets an object at a user-specified object Id.
     * 
     * @param objectId
     *            the object Id.
     * @param object
     *            the object to put into the partition.
     * @param scn
     *            the global scn (equivalent to a time stamp).
     * @throws Exception
     */
    @Override
    public boolean set(int objectId, T object, long scn) throws Exception {
        if (object == null) {
            return delete(objectId, scn);
        }

        _partition.set(objectId, getSerializer().serialize(object), scn);
        return true;
    }

    /**
     * Deletes an object based on a user-specified object Id.
     * 
     * @param objectId
     *            the object Id.
     * @param scn
     *            the global scn (equivalent to a time stamp).
     * @throws Exception
     */
    @Override
    public boolean delete(int objectId, long scn) throws Exception {
        _partition.delete(objectId, scn);
        return true;
    }

    /**
     * Sync this object partition.
     * 
     * @throws IOException
     */
    @Override
    public void sync() throws IOException {
        _partition.sync();
    }

    /**
     * Persists this object partition.
     * 
     * @throws IOException
     */
    @Override
    public void persist() throws IOException {
        _partition.persist();
    }

    /**
     * Clears this object partition by removing all the persisted data permanently.
     * 
     * @throws IOException
     */
    @Override
    public void clear() {
        _partition.clear();
    }

    /**
     * @return the high water mark.
     */
    @Override
    public long getHWMark() {
        return _partition.getHWMark();
    }

    /**
     * @return the low water mark.
     */
    @Override
    public long getLWMark() {
        return _partition.getLWMark();
    }

    /**
     * Saves the high water mark.
     */
    @Override
    public void saveHWMark(long endOfPeriod) throws Exception {
        _partition.saveHWMark(endOfPeriod);
    }

    /**
     * Gets an object in raw bytes based on a user-specified object Id.
     * 
     * @param objectId
     *            the Id of an object to be retrieved from the partition.
     * @return an object in raw bytes according to the given object Id.
     */
    @Override
    public byte[] getBytes(int objectId) {
        return _partition.get(objectId);
    }

    @Override
    public void expandCapacity(int index) throws Exception {
        if (this instanceof DynamicArray) {
            ((DynamicArray) this).expandCapacity(index);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean hasIndex(int index) {
        return _partition.hasIndex(index);
    }

    @Override
    public int length() {
        return _partition.length();
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

    @Override
    public final Type getType() {
        return _partition.getType();
    }
}
