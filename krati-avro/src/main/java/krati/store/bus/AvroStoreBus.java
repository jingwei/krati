/*
 * Copyright (c) 2011 LinkedIn, Inc
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

package krati.store.bus;

import krati.retention.Retention;
import krati.store.avro.AvroStore;

import org.apache.avro.generic.GenericRecord;

/**
 * AvroStoreBus
 * 
 * @author jwu
 * @since 09/21, 2011
 */
public interface AvroStoreBus<K> extends StoreBus<K, GenericRecord> {
    
    /**
     * @return the underlying AvroStore.
     */
    public AvroStore<K> getStore();
    
    /**
     * @return the underlying Retention.
     */
    public Retention<K> getRetention();
}
