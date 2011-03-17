package krati.core.array.entry;

import java.util.List;
import java.util.Arrays;

/**
 * EntryUtility
 * 
 * @author jwu
 * 
 */
public class EntryUtility {
    
    /**
     * Sort all the EntryValue(s) from an Entry array into an array of
     * EntryValue(s).
     * 
     * @param <T>
     *            a generic type for EntryValue.
     * @param entryArray
     *            an Entry array from which EntryValue(s) will be sorted.
     *            Returns null if the entryList is null or it does not contain
     *            any values
     * 
     * @return an array of EntryValue(s) of generic type <T>.
     * 
     * @throws NullPointerException
     *             if the first Entry in the array is null.
     */
    public static <T extends EntryValue> T[] sortEntriesToValues(Entry<T>[] entryArray) {
        int len = 0;
        T[] valArray = null;
        
        if (entryArray == null) {
            return valArray;
        }
        
        for (Entry<T> e : entryArray) {
            len += e.size();
        }
        
        if (len == 0) {
            return valArray;
        }
        
        // Create valArray
        valArray = entryArray[0].getValueFactory().newValueArray(len);
        
        // Add values from entries to valArray
        int i = 0;
        for (Entry<T> e : entryArray) {
            for (T val : e.getValueList()) {
                valArray[i++] = val;
            }
        }
        
        // Sort values in valArray
        Arrays.sort(valArray);
        return valArray;
    }
    
    /**
     * Sort all the EntryValue(s) from an Entry list into an array of
     * EntryValue(s).
     * 
     * @param <T>
     *            a generic type for EntryValue.
     * @param entryList
     *            an Entry list from whose EntryValue(s) will be sorted.
     * 
     * @return an array of EntryValue(s) of generic type <T>. Returns null if
     *         the entryList is null or it does not contain any values
     * 
     * @throws NullPointerException
     *             if the first Entry in the list is null.
     */
    public static <T extends EntryValue> T[] sortEntriesToValues(List<Entry<T>> entryList) {
        int len = 0;
        T[] valArray = null;
        
        if (entryList == null) {
            return valArray;
        }
        
        for (Entry<T> e : entryList) {
            len += e.size();
        }
        
        if (len == 0) {
            return valArray;
        }
        
        // Create valArray
        valArray = entryList.get(0).getValueFactory().newValueArray(len);
        
        // Add values from entries to valArray
        int i = 0;
        for (Entry<T> e : entryList) {
            for (T val : e.getValueList()) {
                valArray[i++] = val;
            }
        }
        
        // Sort values in valArray
        Arrays.sort(valArray);
        return valArray;
    }
}
