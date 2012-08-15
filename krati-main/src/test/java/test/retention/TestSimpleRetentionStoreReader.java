package test.retention;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.io.Serializer;
import krati.io.serializer.StringSerializerUtf8;
import krati.retention.*;
import krati.retention.clock.Clock;
import krati.retention.clock.ClockSerializer;
import krati.retention.clock.SourceWaterMarksClock;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;
import krati.store.DataStore;
import krati.store.factory.DynamicObjectStoreFactory;
import krati.store.factory.ObjectStoreFactory;
import krati.util.SourceWaterMarks;
import org.junit.After;
import org.junit.Before;
import test.util.DirUtils;


import java.io.File;
import java.util.*;

/**
 * @author spike(alperez)
 */
public class TestSimpleRetentionStoreReader extends TestCase {
    private final static String source1 = "source1";
    protected List<String> _sources;

    private SourceWaterMarksClock clock;
    private RetentionStoreWriter<String,String> writer;
    private SimpleRetentionStoreReader<String,String> reader;

    protected File getHomeDir() {
        return DirUtils.getTestDir(getClass());
    }

    public void testBootstrap() throws Exception
    {
        int numberOfKeys = 3;
        long scn = 123456;

        HashSet<String> keySet = new HashSet<String>();
        for(int i = 0; i < numberOfKeys; i++) {
            String key = "key " + i;
            String value1 = "value 1" + i;
            writer.put(key,value1,scn++);
            keySet.add(key);
        }
        System.out.println("Number of records inserted: " + numberOfKeys);

        List<Event<String>> eventList = new ArrayList<Event<String>>();
        HashSet<String> retrievedKeySet = new HashSet<String>();
        Position pos = reader.getPosition(Clock.ZERO);
        int resultCnt = 0;
        do {
            eventList.clear();
            System.out.println("Position: " +  pos);
            pos = reader.get(pos, eventList);
            System.err.println("event list: " + eventList);
            for (Event<String> event : eventList) {
                retrievedKeySet.add(event.getValue());
            }
        } while(eventList.size() > 0);
        System.err.println("------------------------");
        System.err.println("Original key set");
        System.err.println(keySet);
        System.err.println("Retrieved key set");
        System.err.println(retrievedKeySet);
        System.err.println("resultCnt: " + resultCnt);
        System.err.println("------------------------");
        assertEquals(keySet.size(),resultCnt);
    }


    @Override
    @Before
    protected void setUp() throws Exception {
        try {
            DirUtils.deleteDirectory(getHomeDir());
        }catch (Exception e) {
            e.printStackTrace();
        }
        // Build a composite retention reader upon two retentions each with one store
        // Use two writer threads to update the underlying stores and the respected retentions
        // Let a compositeRetentionReader to read from the retention from
        //  (1) Clock.ZERO: then all updates should be read by the client.

        Retention<String> retention1 = createRetention(1);

        DataStore<String,String> store1 = createStore("s1");

        clock = getClock("source1WaterMarks.scn", Lists.newArrayList(source1));

        writer = new SimpleRetentionStoreWriter<String,String>(source1,retention1,store1,clock);

        reader = new SimpleRetentionStoreReader<String,String>(source1,retention1,store1);
    }

    @Override
    @After
    protected void tearDown() {
        try {
            DirUtils.deleteDirectory(getHomeDir());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected SourceWaterMarksClock getClock(String clockFileName,List<String> sources)
    {
        File sourceWaterMarksFile = new File(getHomeDir(), clockFileName);
        SourceWaterMarks sourceWaterMarks = new SourceWaterMarks(sourceWaterMarksFile);
        return new SourceWaterMarksClock(sources, sourceWaterMarks);
    }

    protected Retention<String> createRetention(int id) throws Exception {
        RetentionConfig<String> config = new RetentionConfig<String>(id, getHomeDir());
        config.setBatchSize(getEventBatchSize());
        config.setRetentionPolicy(createRetentionPolicy());
        config.setEventValueSerializer(createEventValueSerializer());
        config.setEventClockSerializer(createEventClockSerializer());
        config.setRetentionSegmentFileSizeMB(16);
        config.setClockSize(1);

        return new SimpleRetention<String>(config);
    }

    /**
     * 
     * @param storeName
     * @return a store of key type String mapping to value type String
     * @throws Exception
     */
    protected DataStore<String, String> createStore(String storeName) throws Exception {
        StoreConfig config = new StoreConfig(new File(getHomeDir(), storeName), 10000);
        config.setSegmentFileSizeMB(16);
        config.setNumSyncBatches(10);
        config.setBatchSize(100);

        ObjectStoreFactory<String, String> factory = new DynamicObjectStoreFactory<String, String>();
        return factory.create(config, new StringSerializerUtf8(), new StringSerializerUtf8());
    }

    protected int getEventBatchSize() {
        return 100;
    }

    protected int getNumRetentionBatches() {
        return 3;
    }

    protected RetentionPolicy createRetentionPolicy() {
        return new RetentionPolicyOnSize(getNumRetentionBatches());
    }

    protected EventBatchSerializer<String> createBatchSerializer() {
        return new SimpleEventBatchSerializer<String>(createEventValueSerializer(), createEventClockSerializer());
    }

    protected Serializer<Clock> createEventClockSerializer() {
        return new ClockSerializer();
    }

    protected Serializer<String> createEventValueSerializer() {
        return new StringSerializerUtf8();
    }

    protected String nextKey() {
        return UUID.randomUUID().toString();
    }

    protected String nextValue() {
        return "value." + UUID.randomUUID().toString();
    }

}
