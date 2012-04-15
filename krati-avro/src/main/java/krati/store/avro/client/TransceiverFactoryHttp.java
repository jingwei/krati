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

package krati.store.avro.client;

import java.net.URL;

import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;

/**
 * TransceiverFactoryHttp
 * 
 * @author jwu
 * @since 09/27, 2011
 */
public class TransceiverFactoryHttp implements TransceiverFactory {
    private URL _url;
    
    public TransceiverFactoryHttp(URL url) {
        this._url = url;
    }
    
    @Override
    public Transceiver newTransceiver() {
        return new HttpTransceiver(_url);
    }
    
    public final URL getURL() {
        return _url;
    }
    
    public void setURL(URL url) {
        this._url = url;
    }
}
