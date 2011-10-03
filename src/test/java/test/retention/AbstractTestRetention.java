package test.retention;

import java.io.File;
import java.util.Random;

import test.retention.util.RandomClockFactory;
import test.util.DirUtils;

import junit.framework.TestCase;
import krati.io.Serializer;
import krati.retention.Retention;
import krati.retention.Event;
import krati.retention.EventBatchSerializer;
import krati.retention.Position;
import krati.retention.SimpleEventBatchSerializer;
import krati.retention.clock.Clock;
import krati.retention.clock.ClockSerializer;
import krati.retention.clock.Occurred;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;

/**
 * AbstractTestRetention
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/09, 2011 - Created
 */
public abstract class AbstractTestRetention<T> extends TestCase {
    protected Retention<T> _retention;
    protected Random _rand = new Random();
    protected RandomClockFactory _clockFactory = new RandomClockFactory(3);
    
    protected File getHomeDir() {
        return DirUtils.getTestDir(getClass());
    }

    protected int getId() {
        return 2;
    }
    
    protected int getEventBatchSize() {
        return 1000;
    }
    
    protected int getNumRetentionBatches() {
        return 20;
    }
    
    protected RetentionPolicy createRetentionPolicy() {
        return new RetentionPolicyOnSize(getNumRetentionBatches());
    }
    
    protected EventBatchSerializer<T> createBatchSerializer() {
        return new SimpleEventBatchSerializer<T>(createEventValueSerializer(), createEventClockSerializer());
    }
    
    protected Serializer<Clock> createEventClockSerializer() {
        return new ClockSerializer();
    }
    
    protected abstract Serializer<T> createEventValueSerializer();
    
    protected abstract Retention<T> createRetention() throws Exception;
    
    protected abstract Event<T> nextEvent(Clock clock);
    
    @Override
    protected void setUp() {
        try {
            DirUtils.deleteDirectory(getHomeDir());
            _retention = createRetention();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _retention.close();
            DirUtils.deleteDirectory(getHomeDir());
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _retention = null;
        }
    }
    
    public void testApi() throws Exception {
        int cnt = getEventBatchSize() + _rand.nextInt(getNumRetentionBatches() * getEventBatchSize() / 2);
        Clock clock = _clockFactory.next();
        Clock minClock = clock;
        _retention.put(nextEvent(clock));
        
        // Create an idle clock
        Clock idleClock0 = _clockFactory.next();
        
        clock = _clockFactory.next();
        _retention.put(nextEvent(clock));
        
        // Create an idle clock
        Clock idleClock1 = _clockFactory.next();
        
        for(int i = 2; i < cnt; i++) {
            clock = _clockFactory.next();
            _retention.put(nextEvent(clock));
        }
        Clock maxClock = clock;
        
        assertEquals(getId(), _retention.getId());
        
        Position pos = _retention.getPosition();
        assertEquals((long)cnt, pos.getOffset());
        assertEquals(cnt, _retention.getOffset());
        
        assertTrue(minClock.compareTo(_retention.getMinClock()) == Occurred.EQUICONCURRENTLY);
        assertTrue(maxClock.compareTo(_retention.getMaxClock()) == Occurred.EQUICONCURRENTLY);
        
        Position sincePosition = _retention.getPosition(idleClock0);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(0, sincePosition.getOffset());
        
        sincePosition = _retention.getPosition(idleClock1);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(1, sincePosition.getOffset());
        
        Retention<T> retention2 = createRetention();
        assertTrue(minClock.compareTo(retention2.getMinClock()) == Occurred.EQUICONCURRENTLY);
        assertTrue(retention2.getMaxClock().beforeEqual(_retention.getMaxClock()));
        
        sincePosition = retention2.getPosition(idleClock0);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(0, sincePosition.getOffset());
        
        sincePosition = _retention.getPosition(idleClock1);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(1, sincePosition.getOffset());
    }
}
