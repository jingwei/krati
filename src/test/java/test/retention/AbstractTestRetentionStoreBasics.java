/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

package test.retention;

import krati.retention.RetentionStoreReader;
import krati.retention.RetentionStoreWriter;
import krati.retention.SimpleRetentionStoreReader;
import krati.retention.SimpleRetentionStoreWriter;

/**
 * AbstractTestRetentionStoreBasics
 * 
 * @author jwu
 * @since 11/20, 2011
 */
public abstract class AbstractTestRetentionStoreBasics<K, V> extends AbstractTestRetentionStore<K, V> {
    
    public void testIOBasics() throws Exception {
        RetentionStoreWriter<K, V> writer1 = new SimpleRetentionStoreWriter<K, V>(source1, _retention, _store, _clock);
        RetentionStoreReader<K, V> reader1 = new SimpleRetentionStoreReader<K, V>(source1, _retention, _store);
        
        long scn = System.currentTimeMillis();
        for(int i = 0; i < 10; i++) {
            K key = nextKey();
            V value = nextValue();
            
            writer1.put(key, value, scn++);
            assertTrue(checkValueEquality(value, reader1.get(key)));
            
            writer1.delete(key, scn++);
            assertTrue(null == reader1.get(key));
        }
        assertEquals(scn, writer1.getHWMark() + 1);
        
        writer1.sync();
        assertEquals(writer1.getLWMark(), writer1.getHWMark());
        assertEquals(writer1.getLWMark(), reader1.getPosition().getClock().values()[0]);
        
        K key = nextKey();
        V value = nextValue();
        writer1.put(key, value, scn++);
        assertEquals(scn, writer1.getHWMark() + 1);
        assertTrue(writer1.getLWMark() < writer1.getHWMark());
        
        writer1.persist();
        assertEquals(writer1.getLWMark(), writer1.getHWMark());
        assertEquals(writer1.getLWMark(), reader1.getPosition().getClock().values()[0]);
        
        RetentionStoreWriter<K, V> writer1A = new SimpleRetentionStoreWriter<K, V>(source1, _retention, _store, _clock);
        assertEquals(writer1.getLWMark(), writer1A.getLWMark());
        assertEquals(writer1.getHWMark(), writer1A.getHWMark());
    }
}
