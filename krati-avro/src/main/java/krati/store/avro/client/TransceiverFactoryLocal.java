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

import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.generic.GenericResponder;

/**
 * TransceiverFactoryLocal
 * 
 * @author jwu
 * @since 10/03, 2011
 */
public class TransceiverFactoryLocal implements TransceiverFactory {
    private GenericResponder _responder;
    private Transceiver _transceiver;
    
    public TransceiverFactoryLocal(GenericResponder responder) {
        this._responder = responder;
        this._transceiver = new LocalTransceiver(responder);
    }
    
    public final GenericResponder getResponder() {
        return _responder;
    }
    
    @Override
    public final Transceiver newTransceiver() {
        return _transceiver;
    }
}
