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
 * Date: 8/9/12
 * @author tchen
 */
public class TestCompositeRetentionStoreReader<K,V> extends TestCase {
  protected List<String> _sources;

  private final static String source1 = "source1";
  private final static String source2 = "source2";
  private final static String source3 = "source3";
  
  private SourceWaterMarksClock clock1; //clock for store1
  private SourceWaterMarksClock clock2; //clock for store2
  private SourceWaterMarksClock clock3; //clock for store2

  private RetentionStoreWriter<String,String> writer1, writer2, wreter3;
  private SimpleRetentionStoreReader<String,String> singleReader1, singleReader2, singleReader3;

  protected File getHomeDir() {
    return DirUtils.getTestDir(getClass());
  }

  public void testCompositeRetentionStoreReaderHappyPath() throws Exception
  {
    // Build a composite retention reader upon two retentions each with one store
    // Use two writer threads to update the underlying stores and the respected retentions
    // Let a compositeRetentionReader to read from the retention from
    //  (1) Clock.ZERO: then all updates should be read by the client.

    // Start the writers to populate the stores
    //int cnt = getEventBatchSize() * getNumRetentionBatches();
    int cnt = 3;

    long scn = System.currentTimeMillis();

    HashSet<String> keySet = new HashSet<String>();
    for(int i = 0; i < cnt; i++) {
      String key = "key " + i;
      String value1 = "value 1" + i;
      writer1.put(key,value1,scn++);
      value1 = "value 2" + i;
      //writer2.put(key,value1,scn++);
      keySet.add(key);
    }
    System.out.println("Records inserted: " + cnt);

    // Test the bootstrapping code
    CompositeRetentionStoreReader<String, String> compositeRetentionStoreReader = new CompositeRetentionStoreReader<String, String>
            (Lists.<RetentionStoreReader<String, String>>newArrayList(singleReader1,singleReader2));
    List<Event<String>> list = new ArrayList<Event<String>>();
    
    //Position pos = compositeRetentionStoreReader.getPosition(Clock.ZERO);
    Position pos = singleReader1.getPosition(Clock.ZERO);
    int resultCnt = 0;
    do {
      list.clear();
      System.out.println("Position: " +  pos);
      //pos = compositeRetentionStoreReader.get(pos,list);
      pos = singleReader1.get(pos, list);
      System.err.println("list: " + list);
      resultCnt += list.size();
      System.out.println("Records read: " + resultCnt);
    } while(list.size() > 0);
    System.err.println("------------------------");
    System.err.println(keySet);
    System.err.println("resultCnt: " + resultCnt);
    System.err.println("------------------------");
    assertEquals(keySet.size(),resultCnt);
  }

  public void XXtestCompositeRetentionStoreReaderStreaming() throws Exception {
    // Build a composite retention reader upon two retentions each with one store
    // Use two writer threads to update the underlying stores and the respected retentions
    // Let a compositeRetentionReader to read from the retention from
    //  (1) Clock.ZERO: then all updates should be read by the client.

    Retention<String> retention1 = createRetention(1);
    Retention<String> retention2 = createRetention(2);

    DataStore<String,String> store1 = createStore("s1");
    DataStore<String,String> store2 = createStore("s2");

    clock1 = getClock("source1WaterMarks.scn", Lists.newArrayList(source1));
    clock2 = getClock("source2WaterMarks.scn", Lists.newArrayList(source2));


    RetentionStoreWriter<String,String> writer1 =
            new SimpleRetentionStoreWriter<String,String>(source1,retention1,store1,clock1);
    RetentionStoreWriter<String,String> writer2 =
            new SimpleRetentionStoreWriter<String,String>(source2,retention2,store2,clock2);


    SimpleRetentionStoreReader<String,String> singleReader1 =
            new SimpleRetentionStoreReader<String,String>(source1,retention1,store1);

    SimpleRetentionStoreReader<String,String> singleReader2 =
            new SimpleRetentionStoreReader<String,String>(source2,retention2,store2);

    // Start the writers to populate the stores
    int cnt = getEventBatchSize() * getNumRetentionBatches();

    long scn = System.currentTimeMillis();

    HashSet<String> keySet = new HashSet<String>();
    for(int i = 0; i < cnt; i++)
    {
      String key = nextKey();
      String value1 = nextValue();
      writer1.put(key,value1,scn++);
      value1 = nextValue();
      writer2.put(key,value1,scn++);
      keySet.add(key);
    }
    System.out.println("Records inserted: " + cnt);

    // Test the bootstrapping code
    CompositeRetentionStoreReader compositeRetentionStoreReader = new CompositeRetentionStoreReader
            (Lists.newArrayList(singleReader1,singleReader2));

    List<Event<String>> list = new ArrayList<Event<String>>();

    Position pos = compositeRetentionStoreReader.getPosition(Clock.ZERO);

    int resultCnt = 0;
    do {
      list.clear();
      System.out.println("Position: " +  pos);
      pos = compositeRetentionStoreReader.get(pos,list);
      resultCnt += list.size();
      System.out.println("Records read: " + resultCnt);
    }while(list.size() > 0);

    /*
      Now the reader should catch up and we will test the streaming read
    */
    assertFalse(pos.isIndexed());
    
    HashMap<String,String> hashMap1 = new HashMap<String, String>();
    HashMap<String,String> hashMap2 = new HashMap<String, String>();
    
    for(int i = 0; i < 5; i++)
    {
      String key = nextKey();
      String value = nextValue();
      writer1.put(key,value,scn++);
      writer2.put(key,value,scn++);
      hashMap1.put(key,value);
      hashMap1.put(key,value);
    }
    // Verify that the right number of updates is read;
    list.clear();
    pos = compositeRetentionStoreReader.get(pos,list);
    assertEquals(5,list.size());
    list.clear();
    pos = compositeRetentionStoreReader.get(pos,list);
    assertEquals(5,list.size());

    // Verify that the retention reader returns the right value
    for(String k : hashMap1.keySet())
    {
      assertEquals(hashMap1.get(k),compositeRetentionStoreReader.get(k).get(source1));
    }
    for(String k : hashMap2.keySet())
    {
      assertEquals(hashMap2.get(k),compositeRetentionStoreReader.get(k).get(source2));
    }
  }

  /**
   * This test checks if the client can correctly find itself fall out of the retention
   * @throws Exception
   */

  public void XXtestCompositeRetentionStoreReaderFallOutOfRetention() throws Exception
  {
    // Build a composite retention reader upon three retentions each with one store
    // Use three writer threads to update the underlying stores and the respected retentions
    // Let a compositeRetentionReader to read from the retention 

    Retention<String> retention1 = createRetention(1);
    Retention<String> retention2 = createRetention(2);
    Retention<String> retention3 = createRetention(3);
    
    DataStore<String,String> store1 = createStore("s1");
    DataStore<String,String> store2 = createStore("s2");
    DataStore<String,String> store3 = createStore("s3");

    clock1 = getClock("source1WaterMarks.scn", Lists.newArrayList(source1));
    clock2 = getClock("source2WaterMarks.scn", Lists.newArrayList(source2));
    clock3 = getClock("source3WaterMarks.scn", Lists.newArrayList(source3));


    RetentionStoreWriter<String,String> writer1 =
            new SimpleRetentionStoreWriter<String,String>(source1,retention1,store1,clock1);
    RetentionStoreWriter<String,String> writer2 =
            new SimpleRetentionStoreWriter<String,String>(source2,retention2,store2,clock2);
    RetentionStoreWriter<String,String> writer3 =
            new SimpleRetentionStoreWriter<String,String>(source3,retention3,store3,clock3);


    SimpleRetentionStoreReader<String,String> singleReader1 =
            new SimpleRetentionStoreReader<String,String>(source1,retention1,store1);

    SimpleRetentionStoreReader<String,String> singleReader2 =
            new SimpleRetentionStoreReader<String,String>(source2,retention2,store2);

    SimpleRetentionStoreReader<String,String> singleReader3 =
            new SimpleRetentionStoreReader<String,String>(source3,retention3,store3);

    
    // Start the writers to populate the stores
    int cnt = getEventBatchSize() * getNumRetentionBatches();

    long scn = System.currentTimeMillis();

    HashSet<String> keySet = new HashSet<String>();
    for(int i = 0; i < cnt; i++)
    {
      String key = nextKey();
      String value1 = nextValue();
      writer1.put(key,value1,scn++);
      value1 = nextValue();
      writer2.put(key,value1,scn++);
      value1 = nextValue();
      writer3.put(key,value1,scn++);
      keySet.add(key);
    }
    System.out.println("Records inserted: " + cnt);

    // Test the bootstrapping code
    CompositeRetentionStoreReader compositeRetentionStoreReader = new CompositeRetentionStoreReader
            (ImmutableList.of(singleReader1,singleReader2,singleReader3));

    List<Event<String>> list = new ArrayList<Event<String>>();

    Position pos = compositeRetentionStoreReader.getPosition(Clock.ZERO);

    int resultCnt = 0;
    do {
      list.clear();
      System.out.println("Position: " +  pos);
      pos = compositeRetentionStoreReader.get(pos,list);
      resultCnt += list.size();
      System.out.println("Records read: " + resultCnt);
    }while(list.size() > 0);

    /*
      Now the reader should catch up and we will test the streaming read
    */
    assertFalse(pos.isIndexed());
    
    // Insert another batch of records so that the client is out of retention again
    for(int i = 0; i < getEventBatchSize() * getNumRetentionBatches();i++)
    {
      writer1.put(nextKey(),nextValue(),scn++);
      writer2.put(nextKey(),nextValue(),scn++);
    }

    // Test if the client is out of retention
    pos = compositeRetentionStoreReader.getPosition(pos.getClock());
    assertNull(pos);
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
    Retention<String> retention2 = createRetention(2);

    DataStore<String,String> store1 = createStore("s1");
    DataStore<String,String> store2 = createStore("s2");

    clock1 = getClock("source1WaterMarks.scn", Lists.newArrayList(source1));
    clock2 = getClock("source2WaterMarks.scn", Lists.newArrayList(source2));

    writer1 = new SimpleRetentionStoreWriter<String,String>(source1,retention1,store1,clock1);
    writer2 = new SimpleRetentionStoreWriter<String,String>(source2,retention2,store2,clock2);

    singleReader1 = new SimpleRetentionStoreReader<String,String>(source1,retention1,store1);
    singleReader2 = new SimpleRetentionStoreReader<String,String>(source2,retention2,store2);

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
