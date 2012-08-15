package krati.retention;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import krati.retention.clock.Clock;

/**
 * A CompositeRetentionStoreReader is a RetentionStoreReader that can be created on top
 * of individual RetentionStoreReaders. This allows to have individual retentions for
 * individual stores, and since this class is stateless (in the sense that doesn't save
 * data) it's possible to create ad-hoc joiners over any combination of data stores.
 * @author spike(alperez)
 */
public class CompositeRetentionStoreReader<K, V> extends
AbstractRetentionStoreReader<K, Map<String, V>> {
    private final ArrayList<RetentionStoreReader<K, V>> stores;

    public CompositeRetentionStoreReader(List<RetentionStoreReader<K, V>> stores) {
        checkNotNull(stores);
        checkArgument(stores.size() > 0);
        checkArgument(storeSourcesUnique(stores));

        Set<String> sources = new HashSet<String>(stores.size());
        for (RetentionStoreReader<K, V> rsr : stores) {
            if (sources.contains(rsr.getSource())) {
                throw new IllegalArgumentException("Retention name " + rsr.getSource()
                        + " is duplicated.");
            }
            sources.add(rsr.getSource());
        }

        this.stores = new ArrayList<RetentionStoreReader<K, V>>(stores);
    }

    /**
     * Argument check helper.
     * 
     * @return true if all the passed readers have unique source names (as
     *         returned by getSource).
     */
    private boolean storeSourcesUnique(List<RetentionStoreReader<K, V>> ss) {
        Set<String> names = new HashSet<String>();
        for (RetentionStoreReader<?, ?> s : ss) {
            names.add(s.getSource());
        }
        return names.size() == ss.size();
    }

    @Override
    public Position getPosition() {
        ArrayList<Position> pos = new ArrayList<Position>(stores.size());
        for (RetentionStoreReader<K, V> store : stores) {
            pos.add(store.getPosition());
        }
        return new CompositePosition(getClockDimension(), pos);
    }

    @Override
    public Position getPosition(Clock sinceClock) {
        checkNotNull(sinceClock);
        checkArgument(sinceClock.dimension() == 0 || sinceClock.dimension() == getClockDimension());
        ArrayList<Position> pos = new ArrayList<Position>(stores.size());
        int index = 0;
        boolean rewrite = false;
        if (sinceClock.equals(Clock.ZERO)) {
            rewrite = true;
        } else {
            for (RetentionStoreReader<K, V> store : stores) {
                int nextIndex = index + store.getClockDimension();
                Clock newClock = new Clock(Arrays.copyOfRange(sinceClock.values(), index, nextIndex));
                Position newPos = store.getPosition(newClock);
                if (newPos.isIndexed() && index != 0) {
                    rewrite = true;
                    break;
                }
                index = nextIndex;
                pos.add(store.getPosition(newClock));
            }
        }
        // If one store that's not the first falls into bootstrapping mode,
        // we'll reset the position
        // as bootstrapping from the first, and to the present for all the rest.
        if (rewrite) {
            pos = new ArrayList<Position>(stores.size());
            pos.add(stores.get(0).getPosition(Clock.ZERO)); // bootstrap from
            // the first store
            for (index = 1; index < stores.size(); index++) {
                pos.add(stores.get(index).getPosition());// current position all
                // the rest
            }
        }
        return new CompositePosition(getClockDimension(), pos);
    }

    @Override
    public Position get(Position pos, List<Event<K>> list) {
        checkArgument(pos instanceof CompositePosition, "pos does not subclass CompositePosition.");
        CompositePosition cp = (CompositePosition) pos;
        checkArgument(cp.dimension() == stores.size());

        Position[] pp = cp.getPositions();
        for (int i = 0; i < stores.size(); i++) {
            List<Event<K>> tList = new LinkedList<Event<K>>();
            Position np = stores.get(i).get(pp[i], tList);
            if (tList.size() > 0) {
                list.addAll(tList);
                pp[i] = np;
                // TODO: assert pp[i] not equal np, or else we'll never finish.
                return new CompositePosition(getClockDimension(), pp);
            }
        }
        // if we're here, we don't have any updates.
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
        for (RetentionStoreReader<K, V> rsr : stores) {
            retVal.put(rsr.getSource(), rsr.get(key));
        }
        return retVal;
    }

    @Override
    public int getClockDimension() {
        int dim = 0;
        for (RetentionStoreReader<K, V> store : stores) {
            dim += store.getClockDimension();
        }
        return dim;
    }
}
