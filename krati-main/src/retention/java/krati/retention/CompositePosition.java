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
import java.util.LinkedList;
import java.util.List;

import krati.retention.clock.Clock;

/**
 * 
 * @author spike(alperez)
 *
 */
public class CompositePosition implements Position {
    private static final long serialVersionUID = 1L;
    private final Position[] positions;
    
    @Override
    public boolean equals(Object o) {
        if (null == o) return false;
        if (o.getClass() != this.getClass()) return false;
        CompositePosition p = (CompositePosition) o;
        return Arrays.equals(this.positions, p.positions);
    }
    
    @Override
    public int hashCode() {
        return positions.hashCode();
    }
    
    public CompositePosition(Position... positions) {
        //TODO: verify size > 0
        this.positions = positions;
    }
    
    public CompositePosition(List<Position> positions) {
        //TODO: verify size > 0
        this.positions = (Position []) positions.toArray();
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
        //O solo el primero.
        for (Position p : positions) {
            if (p.isIndexed()) return true;
        }
        return false;
    }
    
    public int dimension() {
        int d = 0;
        for (Position p : positions){
            d += p.getClock().values().length;
        }
        return d;
    }
    
    public Position getPosition(int index) {
        //TODO: validate index < dimension
        return positions[index];
    }
    
    public Position[] getPositions() {
        return positions;
    }
    
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
        for (int i=0; i < positions.length; i++) {
            b.append("id= ").append(positions[i].getId())
            .append(", offset= ").append(positions[i].getOffset())
            .append(", index= ").append(positions[i].getIndex())
            .append(", clock= ").append(positions[i].getClock());
            if (positions.length != i+1) b.append(" | ");
        }
        return b.toString();
    }
    
    public static CompositePosition parsePosition(String s) {
        String[] parts = s.split("\\|");
        SimplePosition[] positions = new SimplePosition[parts.length];
        for (int i = 0; i < positions.length; i++) {
            String[] fields = parts[i].split("=|,");
            String[] clockParts = fields[7].split(":");
            long[] clockNumbers = new long[clockParts.length];
            for (int j=0; j< clockParts.length; j++) {
                clockNumbers[j] = Long.parseLong(clockParts[j].trim());
            }
            Clock clock = new Clock(clockNumbers);
            positions[i] = new SimplePosition(Integer.parseInt(fields[1].trim()), Long.parseLong(fields[3].trim()), Integer.parseInt(fields[5].trim()), clock);
        }
        return new CompositePosition(positions);
    }
}
