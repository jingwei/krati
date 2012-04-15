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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.apache.avro.Protocol;
import org.apache.log4j.Logger;

/**
 * Protocols
 * 
 * @author jwu
 * @since 09/28, 2011
 */
public final class Protocols {
    static Logger _logger = Logger.getLogger(Protocols.class);
    static Protocol _protocol;
    
    static {
        String sep = "/";
        String packagePath = Protocols.class.getPackage().getName().replaceAll("\\.", sep);
        String resourcePath = sep + packagePath + sep + "protocol.avpr";
        
        InputStream is = Protocols.class.getResourceAsStream(resourcePath);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        try {
            int len = 0;
            byte[] buffer = new byte[1024];
            
            while((len = is.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            out.flush();
            out.close();
            
            _protocol = Protocol.parse(new String(out.toByteArray(), "UTF-8"));
            _logger.info(_protocol.toString(true));
        } catch(Exception e) {
            _logger.fatal("Failed to load " + resourcePath, e);
        }
    }
    
    public synchronized static Protocol getProtocol() {
        return _protocol;
    }
}
