/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

import krati.retention.Retention;
import krati.retention.RetentionConfig;
import krati.retention.SimpleEventBus;
import krati.retention.clock.Clock;
import krati.store.factory.DynamicObjectStoreFactory;

/**
 * TestRetentionStoreReader2
 * 
 * @author jwu
 * @since 11/20, 2011
 */
public class TestRetentionStoreReader2 extends TestRetentionStoreReader {
    
    @Override
    protected Retention<String> createRetention() throws Exception {
        RetentionConfig<String> config = new RetentionConfig<String>(getId(), getHomeDir());
        config.setBatchSize(getEventBatchSize());
        config.setRetentionPolicy(createRetentionPolicy());
        config.setEventValueSerializer(createEventValueSerializer());
        config.setEventClockSerializer(createEventClockSerializer());
        config.setSnapshotClockStoreFactory(new DynamicObjectStoreFactory<String, Clock>());
        config.setRetentionSegmentFileSizeMB(16);
        config.setSnapshotSegmentFileSizeMB(16);
        
        return new SimpleEventBus<String>(config);
    }
}
