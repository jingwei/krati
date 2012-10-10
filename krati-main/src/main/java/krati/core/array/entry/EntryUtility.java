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

import java.util.Collections;
import java.util.Comparator;
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
    
    /**
     * Sort entries in the ascending order of entry log IDs.
     */
    public static <T extends EntryValue> void sortEntriesById(List<Entry<T>> entryList) {
        if (entryList.size() > 0) {
            Collections.sort(entryList, new Comparator<Entry<?>>() {
                @Override
                public int compare(Entry<?> e1, Entry<?> e2) {
                    long v1 = getEntryId(e1.getFile().getName());
                    long v2 = getEntryId(e2.getFile().getName());
                    return (v1 < v2) ? -1 : ((v1 == v2) ? 0 : 1);
                }
            });
        }
    }
    
    /**
     * Gets the entry ID based on the specified entry file name.
     */
    public static long getEntryId(String entryFileName) {
        int ind1 = entryFileName.indexOf("_") + 1;
        if (ind1 > 0) {
            int ind2 = entryFileName.indexOf("_", ind1);
            if (ind2 > ind1) {
                String str = entryFileName.substring(ind1, ind2);
                try {
                    return Long.parseLong(str);
                } catch (Exception e) {
                }
            }
        }
        
        return 0;
    }
}
