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

package krati.core;

/**
 * InvalidStoreConfigException defines a runtime exception
 * that can be thrown upon an invalid {@link krati.core.StoreConfig StoreConfig}.
 * 
 * @author jwu
 * @since 06/25, 2011
 * 
 */
public class InvalidStoreConfigException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    /**
     * Creates a new instance of InvalidStoreConfigException.
     * 
     * @param message - the message.
     */
    public InvalidStoreConfigException(String message) {
        super(message);
    }
}
