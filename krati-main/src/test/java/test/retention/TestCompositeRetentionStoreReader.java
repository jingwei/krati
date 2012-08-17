package test.retention;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.io.Serializer;
import krati.io.serializer.IntSerializer;
import krati.io.serializer.StringSerializer;
import krati.io.serializer.StringSerializerUtf8;
import krati.retention.*;
import krati.retention.clock.Clock;
import krati.retention.clock.ClockSerializer;
import krati.retention.clock.SourceWaterMarksClock;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;
import krati.store.ArrayStore;
import krati.store.DataStore;
import krati.store.ObjectStore;
import krati.store.SerializableObjectArray;
import krati.store.factory.ArrayStoreFactory;
import krati.store.factory.DynamicObjectStoreFactory;
import krati.store.factory.ObjectStoreFactory;
import krati.store.factory.StaticArrayStoreFactory;
import krati.util.SourceWaterMarks;
import org.junit.After;
import org.junit.Before;
import test.util.DirUtils;


import java.io.File;
import java.util.*;

/**
 * Date: 8/9/12
 * @author tchen
 */
public class TestCompositeRetentionStoreReader<K,V> extends TestCase {
  protected List<String> _sources;

  private final static String source1 = "source1";
  private final static String source2 = "source2";
  private final static String source3 = "source3";
  private final static int storeSize = 10000;
  
  private SourceWaterMarksClock clock1; //clock for store1
  private SourceWaterMarksClock clock2; //clock for store2
  private SourceWaterMarksClock clock3; //clock for store2

  private RetentionStoreWriter<Integer,String> writer1, writer2, wreter3;
  private SimpleRetentionStoreReader<Integer,String> singleReader1, singleReader2, singleReader3;

  private Retention<Integer> retention1;
  private Retention<Integer> retention2;

  private DataStore<Integer,String> store1;
  private DataStore<Integer,String> store2;

  protected File getHomeDir() {
    return DirUtils.getTestDir(getClass());
  }

  public void testCompositeRetentionStoreReaderHappyPath() throws Exception
  {
    // Build a composite retention reader upon two retentions each with one store
    // Use two writer threads to update the underlying stores and the respected retentions
    // Let a compositeRetentionReader to read from the retention from
    //  Clock.ZERO:
    //   (1) all records in the store should be read by the client.
    //   (2) the position becomes non-indexed (meaning it falls into the retention window)
    // Start the writers to populate the stores
    int cnt = 3;
    long scn = 0;

    HashSet<Integer> keySet = new HashSet<Integer>();
    for(int i = 0; i < cnt; i++) {
      String value1 = "value 1" + i;
      writer1.put(i,value1,scn++);
      value1 = "value 2" + i;
      writer2.put(i,value1,scn++);
      keySet.add(i);
    }
    // Test the bootstrapping code
    CompositeRetentionStoreReader<Integer, String> compositeRetentionStoreReader = new 
            CompositeRetentionStoreReader<Integer, String>
            (Lists.<RetentionStoreReader<Integer, String>>newArrayList(singleReader1,singleReader2));
    List<Event<Integer>> list = new ArrayList<Event<Integer>>();
    
    Position pos = compositeRetentionStoreReader.getPosition(Clock.ZERO);
    int resultCnt = 0;
    do {
      list.clear();
      pos = compositeRetentionStoreReader.get(pos,list);
      resultCnt += list.size();
    } while(list.size() > 0);
    
    assertEquals(storeSize,resultCnt);
    assertFalse(pos.isIndexed());
  }

  public void testCompositeRetentionStoreReaderStreaming() throws Exception {
    int cnt = 3;
    long scn = 0;

    HashSet<Integer> keySet = new HashSet<Integer>();
    for(int i = 0; i < cnt; i++) {
      String value1 = "value 1" + i;
      writer1.put(i,value1,scn++);
      value1 = "value 2" + i;
      writer2.put(i,value1,scn++);
      keySet.add(i);
    }
    // Test the bootstrapping code
    CompositeRetentionStoreReader<Integer, String> compositeRetentionStoreReader = new
            CompositeRetentionStoreReader<Integer, String>
            (Lists.<RetentionStoreReader<Integer, String>>newArrayList(singleReader1,singleReader2));
    List<Event<Integer>> list = new ArrayList<Event<Integer>>();

    Position pos = compositeRetentionStoreReader.getPosition(Clock.ZERO);
    int resultCnt = 0;
    do {
      list.clear();
      pos = compositeRetentionStoreReader.get(pos,list);
    } while(list.size() > 0);

    assertFalse(pos.isIndexed());
    cnt = 6;
    for(int i = 0; i < cnt; i++) {
      String value1 = "value 1" + i;
      writer1.put(i,value1,scn++);
      value1 = "value 2" + i;
      writer2.put(i,value1,scn++);
    }

    do {
      list.clear();
      pos = compositeRetentionStoreReader.get(pos,list);
      resultCnt += list.size();
    } while(list.size() > 0);
    assertEquals(cnt * 2,resultCnt);
    
    // Write more records util the retention1 is full
    for(int i = 0; i < 2 * getNumRetentionBatches() * getEventBatchSize(); i++)
    {
      writer1.put(i,"dummy",scn++);
    }
    writer1.sync();
    pos = compositeRetentionStoreReader.getPosition(pos.getClock());
    System.err.println(pos);
    assertEquals(true,pos.isIndexed());

    // Bootstrap again
    resultCnt = 0;
    do {
      list.clear();
      pos = compositeRetentionStoreReader.get(pos,list);
      resultCnt += list.size();
    } while(list.size() > 0);

    assertFalse(pos.isIndexed());
    assertEquals(storeSize,resultCnt);

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

    retention1 = createRetention(1);
    retention2 = createRetention(2);

    store1 = createStore("s1");
    store2 = createStore("s2");

    clock1 = getClock("source1WaterMarks.scn", Lists.newArrayList(source1));
    clock2 = getClock("source2WaterMarks.scn", Lists.newArrayList(source2));

    writer1 = new SimpleRetentionStoreWriter<Integer,String>(source1,retention1,store1,clock1);
    writer2 = new SimpleRetentionStoreWriter<Integer,String>(source2,retention2,store2,clock2);

    singleReader1 = new SimpleRetentionStoreReader<Integer,String>(source1,retention1,store1);
    singleReader2 = new SimpleRetentionStoreReader<Integer,String>(source2,retention2,store2);

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
  
  protected Retention<Integer> createRetention(int id) throws Exception {
    RetentionConfig<Integer> config = new RetentionConfig<Integer>(id, new File(getHomeDir().getAbsolutePath()+"/retention"+id));
    config.setBatchSize(getEventBatchSize());
    config.setRetentionPolicy(createRetentionPolicy());
    config.setEventValueSerializer(createEventValueSerializer());
    config.setEventClockSerializer(createEventClockSerializer());
    config.setRetentionSegmentFileSizeMB(16);
    config.setClockSize(1);

    return new SimpleRetention<Integer>(config);
  }

  /**
   * 
   * @param storeName
   * @return a store of key type String mapping to value type String
   * @throws Exception
   */
  protected ObjectStore<Integer, String> createStore(String storeName) throws Exception {
    StoreConfig config = new StoreConfig(new File(getHomeDir(), storeName), storeSize);
    config.setSegmentFileSizeMB(16);
    config.setNumSyncBatches(10);
    config.setBatchSize(100);

    ArrayStoreFactory factory = new StaticArrayStoreFactory();
    ArrayStore store = factory.create(config);
    return new SerializableObjectArray(store,new StringSerializer()) ;
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

  protected EventBatchSerializer<Integer> createBatchSerializer() {
    return new SimpleEventBatchSerializer<Integer>(createEventValueSerializer(), createEventClockSerializer());
  }

  protected Serializer<Clock> createEventClockSerializer() {
    return new ClockSerializer();
  }

  protected Serializer<Integer> createEventValueSerializer() {
    return new IntSerializer();
  }

  protected String nextKey() {
    return UUID.randomUUID().toString();
  }

  protected String nextValue() {
    return "value." + UUID.randomUUID().toString();
  }

}
