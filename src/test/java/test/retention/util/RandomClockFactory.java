package test.retention.util;

import java.util.Random;

import krati.retention.clock.Clock;

/**
 * RandomClockFactory
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created
 */
public class RandomClockFactory {
    private final Random _rand;
    private final long[] _scnArray;
    
    public RandomClockFactory(int numSources) {
        _rand = new Random();
        _scnArray = new long[numSources];
        for(int i = 0; i < numSources; i++) {
            _scnArray[i] = System.currentTimeMillis();
        }
    }
    
    public Clock next() {
        for(int i = 0; i < _scnArray.length; i++) {
            _scnArray[i] += _rand.nextInt(10) + 1;
        }
        return new Clock((long[])_scnArray.clone());
    }
}
