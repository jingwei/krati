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

package test.retention;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import test.util.DirUtils;
import junit.framework.TestCase;
import krati.io.Serializer;
import krati.retention.EventBatchSerializer;
import krati.retention.Retention;
import krati.retention.SimpleEventBatchSerializer;
import krati.retention.clock.Clock;
import krati.retention.clock.ClockSerializer;
import krati.retention.clock.SourceWaterMarksClock;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;
import krati.store.DataStore;
import krati.util.SourceWaterMarks;

/**
 * AbstractTestRetentionStore
 * 
 * @author jwu
 * @since 10/17, 2011
 */
public abstract class AbstractTestRetentionStore<K, V> extends TestCase {
    protected List<String> _sources;
    protected DataStore<K, V> _store;
    protected Retention<K> _retention;
    protected SourceWaterMarksClock _clock;
    protected Random _rand = new Random();
    
    protected final static String source1 = "source1";
    protected final static String source2 = "source2";
    protected final static String source3 = "source3";
    
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
        return 5;
    }
    
    protected RetentionPolicy createRetentionPolicy() {
        return new RetentionPolicyOnSize(getNumRetentionBatches());
    }
    
    protected EventBatchSerializer<K> createBatchSerializer() {
        return new SimpleEventBatchSerializer<K>(createEventValueSerializer(), createEventClockSerializer());
    }
    
    protected Serializer<Clock> createEventClockSerializer() {
        return new ClockSerializer();
    }
    
    protected abstract Serializer<K> createEventValueSerializer();
    
    protected abstract Retention<K> createRetention() throws Exception;
    
    protected abstract DataStore<K, V> createStore() throws Exception;
    
    protected abstract K nextKey();
    
    protected abstract V nextValue();
    
    protected abstract boolean checkKeyEquality(K key1, K key2);
    
    protected abstract boolean checkValueEquality(V value1, V value2);
    
    @Override
    protected void setUp() {
        try {
            DirUtils.deleteDirectory(getHomeDir());
            _sources = new ArrayList<String>();
            _sources.add(source1);
            _sources.add(source2);
            _sources.add(source3);
            
            File sourceWaterMarksFile = new File(getHomeDir(), "sourceWaterMarks.scn");
            SourceWaterMarks sourceWaterMarks = new SourceWaterMarks(sourceWaterMarksFile);
            _clock = new SourceWaterMarksClock(_sources, sourceWaterMarks);
            _retention = createRetention();
            _store = createStore();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _store.close();
            _retention.close();
            DirUtils.deleteDirectory(getHomeDir());
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _retention = null;
        }
    }
}
