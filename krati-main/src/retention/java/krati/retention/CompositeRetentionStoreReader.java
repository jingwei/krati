package krati.retention;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import krati.retention.clock.Clock;

public class CompositeRetentionStoreReader<K, V> extends AbstractRetentionStoreReader<K, Map<String, V>> {
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
        CompositePosition cp = (CompositePosition) pos;
        //TODO: assert cp.dimension == stores.size
        Position[] pp = cp.getPositions();
        for (int i=0; i < stores.size(); i++) {
            List<Event<K>> tList = new LinkedList<Event<K>>();
            Position np = stores.get(i).get(pp[i], tList);
            if (tList.size() > 0) {
                list.addAll(tList);
                pp[i] = np;
                //TODO: assert pp[i] not equal np, or else we'll never finish.
                return new CompositePosition(pp);
            }
        }
        //if we're here, we don't have any updates.
        return pos;
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
