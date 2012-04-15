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

package krati.store.demo.multitenant;

import java.io.File;

import krati.core.StoreConfig;
import krati.core.segment.MappedSegmentFactory;
import krati.store.avro.protocol.BasicDataStoreResponderFactory;
import krati.store.avro.protocol.MultiTenantStoreResponder;
import krati.store.factory.DataStoreFactory;
import krati.store.factory.IndexedDataStoreFactory;

import org.apache.avro.ipc.HttpServer;

/**
 * MultiTenantStoreHttpServer
 * 
 * @author jwu
 * @since 10/01, 2011
 */
public class MultiTenantStoreHttpServer {
    
    public static void main(String[] args) throws Exception {
        File homeDir = new File(System.getProperty("java.io.tmpdir"), MultiTenantStoreHttpServer.class.getSimpleName());
        
        // Change initialCapacity accordingly for different data sets
        int initialCapacity = 10000;
        
        // Create store configuration template
        StoreConfig configTemplate = new StoreConfig(homeDir, initialCapacity);
        configTemplate.setSegmentCompactFactor(0.68);
        configTemplate.setSegmentFactory(new MappedSegmentFactory());
        configTemplate.setSegmentFileSizeMB(32);
        configTemplate.setNumSyncBatches(2);
        configTemplate.setBatchSize(100);
        
        // Create store responder and server
        DataStoreFactory storeFactory = new IndexedDataStoreFactory();
        MultiTenantStoreResponder storeResponder = new MultiTenantStoreResponder(homeDir, configTemplate, new BasicDataStoreResponderFactory(storeFactory));
        HttpServer server = new HttpServer(storeResponder, 8080);
        server.start();
        server.join();
    }
}
