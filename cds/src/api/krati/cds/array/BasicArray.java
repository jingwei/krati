package krati.cds.array;

/**
 * BasicArray
 * 
 * @author jwu
 *
 * @param <T> Primitive Java Array (e.g., int[], long[]).
 */
public interface BasicArray<T> extends Array
{
    public T getInternalArray();
}
