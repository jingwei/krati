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

package krati.retention;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;
import krati.retention.clock.Clock;

/**
 * A Position composed of a list of individual positions.
 * @see CompositeRetentionStoreReader
 * Class invariant: of the internal positions, only the first one can be indexed. Also, dimension is equal
 * to the sum of all individual clocks, except when the first clock is zero.
 * @author spike(alperez)
 *
 */
public final class CompositePosition implements Position {
    private static final long serialVersionUID = 1L;
    private final Position[] positions;
    private final int dimension;
    
    @Override
    public boolean equals(Object o) {
        if (null == o) return false;
        if (o.getClass() != this.getClass()) return false;
        CompositePosition p = (CompositePosition) o;
        return Arrays.equals(this.positions, p.positions);
    }
    
    /**
     * Checks that no internal position, except maybe the first one are indexed.
     * @return
     */
    private boolean checkClassInvariant() {
        for (int i = 1; i < positions.length; i++) {
            if (positions[i].isIndexed())
                return false;
        }
        int count = 0;
        for (Position p : positions) {
            count += p.getClock().dimension();
        }
        if (positions[0].getClock().equals(Clock.ZERO)) {
            return (dimension > count);
        } else {
            return (dimension == count);
        }
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(positions);
    }
    
    public CompositePosition(int dimension, Position... positions) {
        checkArgument(positions.length > 0);
        checkArgument(dimension > 0);
        this.dimension = dimension;
        this.positions = positions;
        checkArgument(checkClassInvariant());
    }
    
    public CompositePosition(int dimension, List<Position> positions) {
        checkArgument(positions.size() > 0);
        checkArgument(dimension > 0);
        this.positions = positions.toArray(new Position[0]);
        this.dimension = dimension;
        checkArgument(checkClassInvariant());
    }
    
    @Override
    public int getId() {
        return positions[0].getId();
    }
    
    @Override
    public long getOffset() {
        return positions[0].getOffset();
    }
    
    @Override
    public int getIndex() {
        return positions[0].getIndex();
    }
    
    @Override
    public boolean isIndexed() {
        //Given the class invariant, this is equivalent to check all of them.
        return positions[0].isIndexed();
    }
    
    public int dimension() {
        return dimension;
    }
    
    public Position getPosition(int index) {
        checkArgument(index > 0 && index < positions.length);
        return positions[index];
    }
    
    /**
     * This method returns the component positions.
     * It's equal to the list/array that was used for construction.
     * Returns a copy, so modifications to the returned array won't change
     * this instance of Position.
     */
    public Position[] getPositions() {
        return positions.clone();
    }
    
    /**
     * Returns a Clock, composed of all the clocks of the underlying Positions.
     * For example, if this CompositePositions contains 2 positions that return clocks
     * (2:43) and (1:3:1), this method will return a Clock equal to (2:43:1:3:1) 
     */
    @Override
    public Clock getClock() {
        long[] values = new long[dimension()];
        int i = 0;
        for (Position p : positions) {
            for (long v: p.getClock().values()) {
                values[i] = v;
                i++;
            }
        }
        return new Clock(values);
    }
    
    @Override
    public final String toString() {
        StringBuilder b = new StringBuilder();
        b.append("dimension= ").append(dimension).append(" | ");
        for (int i=0; i < positions.length; i++) {
            b.append("id= ").append(positions[i].getId())
            .append(", offset= ").append(positions[i].getOffset())
            .append(", index= ").append(positions[i].getIndex())
            .append(", clock= ").append(positions[i].getClock());
            if (positions.length != i+1) b.append(" | ");
        }
        return b.toString();
    }
    
    /**
     * Parses the output of toString into a new CompositePosition instance.
     * The behavior is undefined if string is of a different format.
     */
    public static CompositePosition parsePosition(String s) {
        checkNotNull(s);
        String[] parts = s.split("\\|");
        int dimension = Integer.parseInt(parts[0].split("=")[1].trim());
        SimplePosition[] positions = new SimplePosition[parts.length];
        for (int i = 1; i < positions.length; i++) {
            String[] fields = parts[i].split("=|,");
            String[] clockParts = fields[7].split(":");
            long[] clockNumbers = new long[clockParts.length];
            for (int j=0; j< clockParts.length; j++) {
                clockNumbers[j] = Long.parseLong(clockParts[j].trim());
            }
            Clock clock = new Clock(clockNumbers);
            positions[i] = new SimplePosition(Integer.parseInt(fields[1].trim()), Long.parseLong(fields[3].trim()), Integer.parseInt(fields[5].trim()), clock);
        }
        return new CompositePosition(dimension, positions);
    }
}
