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

package krati.core.segment;

/**
 * SegmentFileSizeException
 * 
 * @author jwu
 * 
 */
public class SegmentFileSizeException extends SegmentException {
    private final static long serialVersionUID = 1L;
    private final String segFilePath;
    private final int segFileSizeMB;
    private final int expFileSizeMB;

    public SegmentFileSizeException(String segmentFilePath, int segmentFileSizeMB, int expectedFileSizeMB) {
        super("Invalid segment file size in MB: " + segmentFileSizeMB);
        this.segFilePath = segmentFilePath;
        this.segFileSizeMB = segmentFileSizeMB;
        this.expFileSizeMB = expectedFileSizeMB;
    }

    public String getSegmentFilePath() {
        return segFilePath;
    }

    public int getSegmentFileSizeMB() {
        return segFileSizeMB;
    }

    public int getExpectedFileSizeMB() {
        return expFileSizeMB;
    }
}
