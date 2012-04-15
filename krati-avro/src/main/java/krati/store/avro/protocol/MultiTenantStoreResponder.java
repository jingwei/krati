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

package krati.store.avro.protocol;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import krati.core.StoreConfig;
import krati.store.factory.ArrayStoreFactory;
import krati.store.factory.DataStoreFactory;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericResponder;

/**
 * MultiTenantStoreResponder
 * 
 * @author jwu
 * @since 09/30, 2011
 * 
 * <p>
 * 12/06, 2011 - Added new constructors using {@link DataStoreFactory} and {@link ArrayStoreFactory} <br/>
 */
public class MultiTenantStoreResponder extends GenericResponder {
    private final File _homeDir;
    private final StoreConfig _configTemplate;
    private final StoreResponderFactory _responderFactory;
    private final Map<String, StoreResponder> _tenantMap = new ConcurrentHashMap<String, StoreResponder>();
    private final Object _storeInitLock = new Object();
    
    public MultiTenantStoreResponder(File homeDirectory, StoreConfig configTemplate, StoreResponderFactory responderFactory) {
        super(Protocols.getProtocol());
        this._homeDir = homeDirectory;
        this._configTemplate = configTemplate;
        this._responderFactory = responderFactory;
        this.init();
    }
    
    public MultiTenantStoreResponder(File homeDirectory, StoreConfig configTemplate, DataStoreFactory storeFactory) {
        super(Protocols.getProtocol());
        this._homeDir = homeDirectory;
        this._configTemplate = configTemplate;
        this._responderFactory = new BasicDataStoreResponderFactory(storeFactory);
        this.init();
    }
    
    public MultiTenantStoreResponder(File homeDirectory, StoreConfig configTemplate, ArrayStoreFactory storeFactory) {
        super(Protocols.getProtocol());
        this._homeDir = homeDirectory;
        this._configTemplate = configTemplate;
        this._responderFactory = new BasicArrayStoreResponderFactory(storeFactory);
        this.init();
    }
    
    protected void init() {
        if(!_homeDir.exists()) {
            _homeDir.mkdirs();
        }
    }
    
    public final File getHomeDir() {
        return _homeDir;
    }
    
    public final StoreConfig getStoreConfigTemplate() {
        return _configTemplate;
    }
    
    public final StoreResponder getStoreResponder(String source) {
        return _tenantMap.get(source);
    }
    
    public final StoreResponderFactory getStoreResponderFactory() {
        return _responderFactory;
    }
    
    public boolean hasTenant(String source) {
        return source == null ? false : _tenantMap.containsKey(source);
    }
    
    public StoreResponder createTenant(String source) throws Exception {
        StoreResponder responder = _tenantMap.get(source);
        if(responder == null) {
            synchronized(_storeInitLock) {
                responder = _tenantMap.get(source);
                if(responder == null) {
                    responder = createStoreResponder(source);
                }
            }
        }
        
        return responder;
    }
    
    @Override
    public Object respond(Protocol.Message message, Object request) {
        String source = ((GenericRecord)request).get("src").toString();
        if(source == null) {
            throw new AvroRuntimeException("source null");
        }
        
        StoreResponder responder = _tenantMap.get(source);
        if(responder == null) {
            // Create a new store
            if(message.getName().equals(ProtocolConstants.MSG_META)) {
                String opt = ((GenericRecord)request).get("opt").toString();
                StoreDirective directive = StoreDirective.valueOf(opt);
                if(directive == StoreDirective.StoreInit) {
                    synchronized(_storeInitLock) {
                        responder = _tenantMap.get(source);
                        try {
                            if(responder == null) {
                                responder = createStoreResponder(source);
                                return ProtocolConstants.SUC_UTF8;
                            } else {
                                return ProtocolConstants.NOP_UTF8;
                            }
                        } catch (Exception e) {
                            throw new AvroRuntimeException("Failed to create store " + source, e);
                        }
                    }
                }
            }
            throw new AvroRuntimeException("source unknown: " + source);
        }
        
        return responder.respond(message, request);
    }
    
    protected StoreResponder createStoreResponder(String source) throws Exception {
        File storeDir = new File(_homeDir, source);
        if(!storeDir.exists()) {
            storeDir.mkdirs();
        }
        
        File propertiesFile = new File(storeDir, StoreConfig.CONFIG_PROPERTIES_FILE); 
        getStoreConfigTemplate().save(propertiesFile, source);
        StoreConfig config = StoreConfig.newInstance(storeDir);
        StoreResponder responder = _responderFactory.createResponder(config);
        _tenantMap.put(source, responder);
        return responder;
    }
}
