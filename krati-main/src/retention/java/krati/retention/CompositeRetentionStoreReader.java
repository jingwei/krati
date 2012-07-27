package krati.retention;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import krati.retention.clock.Clock;

public class CompositeRetentionStoreReader<K, V> extends RetentionStoreReader<K, Map<String, V>> {
    private final ArrayList<RetentionStoreReader<K, V>> stores;

    public CompositeRetentionStoreReader(List<RetentionStoreReader<K, V>> stores) {
        //TODO: validate stores.length > 0
        Set<String> sources = new HashSet<String>(stores.size());
        for (RetentionStoreReader<K, V> rsr : stores) {
            if (sources.contains(rsr.getSource())) {
                throw new IllegalArgumentException("Retention name " + rsr.getSource() + " is duplicated.");
            }
            sources.add(rsr.getSource());
        }
        //TODO: validate uniqueness of getSource's
        this.stores = new ArrayList<RetentionStoreReader<K, V>>(stores);
    }
    
    @Override
    public Position getPosition() {
        ArrayList<Position> pos = new ArrayList<Position>(stores.size());
        for (RetentionStoreReader<K, V> store : stores) {
            pos.add(store.getPosition());
        }
        return new CompositePosition(pos);
    }

    @Override
    public Position getPosition(Clock sinceClock) {
        ArrayList<Position> pos = new ArrayList<Position>(stores.size());
        for (RetentionStoreReader<K, V> store : stores) {
            pos.add(store.getPosition(sinceClock));
        }
        return new CompositePosition(pos);
    }

    @Override
    public Position get(Position pos, List<Event<K>> list) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSource() {
        StringBuilder b = new StringBuilder();
        for (RetentionStoreReader<K, V> rsr : stores) {
            b.append(rsr.getSource()).append("; ");
        }
        return b.toString();
    }

    @Override
    public Map<String, V> get(K key) throws Exception {
        Map<String, V> retVal = new HashMap<String, V>();
        for (RetentionStoreReader<K,V> rsr : stores) {
            retVal.put(rsr.getSource(), rsr.get(key));
        }
        return retVal;
    }
}
