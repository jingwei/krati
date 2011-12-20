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

package krati.io;

/**
 * SerializationException
 * 
 * An exception is thrown by {#link krati.io.Serializer Serializer} if an object
 * cannot be serialized to a byte array or de-serialized from a byte array.
 * 
 * @author jwu
 * 06/29, 2011
 */
public class SerializationException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public SerializationException(String message) {
        super(message);
    }
    
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
