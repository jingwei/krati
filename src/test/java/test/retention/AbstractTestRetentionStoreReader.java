package test.retention;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import krati.retention.Event;
import krati.retention.Position;
import krati.retention.RetentionStoreReader;
import krati.retention.RetentionStoreWriter;
import krati.retention.SimpleRetentionStoreReader;
import krati.retention.SimpleRetentionStoreWriter;
import krati.retention.clock.Clock;

/**
 * AbstractTestRetentionStoreReader
 * 
 * @author jwu
 * @since 11/20, 2011
 */
public abstract class AbstractTestRetentionStoreReader<K, V> extends AbstractTestRetentionStore<K, V> {
    
    public void testSimpleRetentionStoreReader() throws Exception {
        RetentionStoreWriter<K, V> writer = new SimpleRetentionStoreWriter<K, V>(source1, _retention, _store, _clock);
        RetentionStoreReader<K, V> reader = new SimpleRetentionStoreReader<K, V>(source1, _retention, _store);
        
        // fill all retention batches
        int cnt = getEventBatchSize() * getNumRetentionBatches();
        long scn = System.currentTimeMillis();
        for(int i = 0; i < cnt; i++) {
            K key = nextKey();
            V value = nextValue();
            
            writer.put(key, value, scn++);
            assertTrue(checkValueEquality(value, reader.get(key)));
        }
        
        Position pos = reader.getPosition(Clock.ZERO);
        assertTrue(pos.isIndexed());
        assertEquals(cnt, pos.getOffset());
        assertTrue(pos.getClock() == Clock.ZERO);
        assertEquals(_retention.getId(), pos.getId());
        
        pos = reader.getPosition(reader.getPosition().getClock());
        assertEquals(false, pos.isIndexed());
        assertEquals(reader.getPosition().getOffset(), pos.getOffset() + 1);
        
        // one more put
        K key = nextKey();
        V value = nextValue();
        
        writer.put(key, value, scn++);
        assertTrue(checkValueEquality(value, reader.get(key)));
        cnt++;
        
        pos = reader.getPosition(Clock.ZERO);
        assertTrue(pos.isIndexed());
        assertEquals(cnt, pos.getOffset());
        assertTrue(pos.getClock() == Clock.ZERO);
        assertEquals(_retention.getId(), pos.getId());
        
        pos = reader.getPosition(reader.getPosition().getClock());
        assertEquals(false, pos.isIndexed());
        assertEquals(reader.getPosition().getOffset(), pos.getOffset() + 1);
        
        // Read from the retention
        List<Event<K>> list = new ArrayList<Event<K>>();
        
        pos = reader.getPosition();
        reader.get(pos, list);
        assertEquals(0, list.size());
        list.clear();
        
        pos = reader.getPosition(reader.getPosition().getClock());
        reader.get(pos, list);
        assertEquals(1, list.size());
        list.clear();
        
        // Sync up from Clock.ZERO and check the number of event batches
        int num1 = 0, num2 = 0;
        pos = reader.getPosition(Clock.ZERO);
        do {
            list.clear();
            pos = reader.get(pos, list);
            if(getEventBatchSize() == list.size()) {
                num1++;
            }
            num2++;
        } while(list.size() > 0);
        
        assertEquals(getNumRetentionBatches(), num1);
        assertTrue(num1 < num2);
        
        // Sync up from Clock.ZERO
        int num3 = 0;
        Map<K, Event<V>> map = new HashMap<K, Event<V>>();
        pos = reader.getPosition(Clock.ZERO);
        do {
            map.clear();
            pos = reader.get(pos, map);
            num3++;
        } while(map.size() > 0);
        
        assertEquals(num2, num3);
    }
}
