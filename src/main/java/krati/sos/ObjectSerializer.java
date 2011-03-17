package krati.sos;

/**
 * An interface for object serialization and construction (de-serialization).
 * 
 * @author jwu
 *
 * @param <V> An object to serialize or to construct.
 */
public interface ObjectSerializer<V> {
    /**
     * Serializes an object.
     * 
     * @param object   an object to be serialized by this serializer.
     * @return a byte array which is the raw representation of an object.
     */
    public byte[] serialize(V object);

    /**
     * Constructs (i.e., de-serializes) an object from raw data.
     * 
     * @param binary   raw data from which an object is constructed.
     * @return a constructed object.
     * @throws runtime ObjectConstructionException if the object cannot be constructed from the raw bytes.
     */
    public V construct(byte[] binary) throws ObjectConstructionException;
}
