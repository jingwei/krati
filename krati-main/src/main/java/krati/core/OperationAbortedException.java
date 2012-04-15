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

package krati.core;

/**
 * OperationAbortedException defines a runtime exception
 * that can be thrown upon an aborted operation. 
 * 
 * @author jwu
 * @since 06/12, 2011
 * 
 */
public class OperationAbortedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    /**
     * Creates a new instance of OperationAbortedException.
     */
    public OperationAbortedException() {
        super("Operation aborted");
    }
    
    /**
     * Creates a new instance of OperationAbortedException.
     * 
     * @param message - the message
     */
    public OperationAbortedException(String message) {
        super(message);
    }
    
    /**
     * Creates a new instance of OperationAbortedException the specified cause.
     * 
     * @param cause   - the cause (which is saved for later retrieval by the {@link RuntimeException#getCause()} method).
     * (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public OperationAbortedException(Throwable cause) {
        super("Operation aborted", cause);
    }
    
    /**
     * 
     * Creates a new instance of OperationAbortedException the specified message and cause.
     * 
     * @param message - the message
     * @param cause   - the cause (which is saved for later retrieval by the {@link RuntimeException#getCause()} method).
     * (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public OperationAbortedException(String message, Throwable cause) {
        super(message, cause);
    }
}
