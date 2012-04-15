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

package krati.store.avro.client;

import java.net.URL;

import krati.io.Serializer;

/**
 * StoreClientHttp
 * 
 * @author jwu
 * @since 09/28, 2011
 */
public class StoreClientHttp<K, V> extends StoreClientImpl<K, V> {
    protected URL _serverURL;
    
    public StoreClientHttp(URL serverURL, String source, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(source, keySerializer, valueSerializer, new TransceiverFactoryHttp(serverURL));
        this._serverURL = serverURL;
    }
    
    public URL getServerURL() {
        return _serverURL;
    }
}
