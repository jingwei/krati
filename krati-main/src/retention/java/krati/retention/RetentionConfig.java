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

import java.io.File;

import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.io.Serializer;
import krati.retention.clock.Clock;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;

/**
 * RetentionConfig
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/12, 2011 - Created <br/>
 */
public class RetentionConfig<T> {
    /**
     * Retention Id.
     */
    private int _id;                                                 // required
    
    /**
     * Retention home directory.
     */
    private File _homeDir;                                           // required
    
    /**
     * Event batch size (the number of events in one {@link EventBatch}).
     */
    private int _batchSize = EventBatch.DEFAULT_BATCH_SIZE;
    
    /**
     * Number of event batches needed to sync updates to <tt>indexes.dat</tt>.
     * The default value is <code>10</code>.
     */
    private int _numSyncBatchs = 10;
    
    /**
     * Retention store initial size.
     */
    private int _retentionInitialSize = RETENTION_INITIAL_SIZE_DEFAULT;
    
    /**
     * Retention store segmentFileSizeMB.
     */
    private int _retentionSegmentFileSizeMB = 32;
    
    /**
     * Retention segment factory.
     */
    private SegmentFactory _retentionSegmentFactory = new WriteBufferSegmentFactory();
    
    /**
     * Retention policy.
     */
    private RetentionPolicy _retentionPolicy = new RetentionPolicyOnSize(1000);
    
    /**
     * {@link krati.retention.Event Event} value serializer.
     */
    private Serializer<T> _eventValueSerializer;                     // required

    /**
     * {@link krati.retention.Event Event} clock serializer.
     */
    private Serializer<Clock> _eventClockSerializer;                 // required
    
    /**
     * The min retention initial size (1000 event batches).
     */
    private final static int RETENTION_INITIAL_SIZE_MIN = 1000;
    
    /**
     * The default retention initial size (10000 event batches).
     */
    private final static int RETENTION_INITIAL_SIZE_DEFAULT = 10000;
    
    /**
     * Creates a new instance of RetentionConfig.
     * 
     * @param id      - the Retention Id
     * @param homeDir - the Retention home directory
     */
    public RetentionConfig(int id, File homeDir) {
        this._id = id;
        this._homeDir = homeDir;
    }
    
    public int getId() {
        return _id;
    }
    
    public File getHomeDir() {
        return _homeDir;
    }
    
    public void setRetentionInitialSize(int retentionInitialSize) {
        this._retentionInitialSize = Math.max(RETENTION_INITIAL_SIZE_MIN, retentionInitialSize);
    }
    
    public int getRetentionInitialSize() {
        return _retentionInitialSize;
    }
    
    public void setRetentionSegmentFileSizeMB(int retentionSegmentFileSizeMB) {
        this._retentionSegmentFileSizeMB = Math.max(Segment.minSegmentFileSizeMB, retentionSegmentFileSizeMB);
    }
    
    public int getRetentionSegmentFileSizeMB() {
        return _retentionSegmentFileSizeMB;
    }
    
    public void setRetentionSegmentFactory(SegmentFactory retentionSegmentFactory) {
        this._retentionSegmentFactory = retentionSegmentFactory == null ?
                new WriteBufferSegmentFactory() : retentionSegmentFactory;
    }
    
    public SegmentFactory getRetentionSegmentFactory() {
        return _retentionSegmentFactory;
    }
    
    public void setBatchSize(int batchSize) {
        this._batchSize = Math.max(EventBatch.MINIMUM_BATCH_SIZE, batchSize);
    }
    
    public int getBatchSize() {
        return _batchSize;
    }
    
    public void setNumSyncBatchs(int numSyncBatchs) {
        this._numSyncBatchs = numSyncBatchs;
    }
    
    public int getNumSyncBatchs() {
        return _numSyncBatchs;
    }
    
    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        this._retentionPolicy = retentionPolicy;
    }
    
    public RetentionPolicy getRetentionPolicy() {
        return _retentionPolicy;
    }
    
    public void setEventValueSerializer(Serializer<T> eventValueSerializer) {
        this._eventValueSerializer = eventValueSerializer;
    }
    
    public Serializer<T> getEventValueSerializer() {
        return _eventValueSerializer;
    }
    
    public void setEventClockSerializer(Serializer<Clock> eventClockSerializer) {
        this._eventClockSerializer = eventClockSerializer;
    }
    
    public Serializer<Clock> getEventClockSerializer() {
        return _eventClockSerializer;
    }
}
