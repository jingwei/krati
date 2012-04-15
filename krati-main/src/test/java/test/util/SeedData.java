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

package test.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import test.AbstractTest;

/**
 * SeedData
 * 
 * @author jwu
 * 05/09, 2011
 */
public class SeedData {
    
    private final List<String> _lines = new ArrayList<String>(10000);
    
    public final List<String> getLines() {
        return _lines;
    }
    
    public void load(File dataFile) throws IOException {
        String line;
        FileReader reader = new FileReader(dataFile);
        BufferedReader in = new BufferedReader(reader);
        
        while((line = in.readLine()) != null) {
            _lines.add(line);
        }
        
        in.close();
        reader.close();
    }
    
    public void load() throws IOException {
        if(_lines.size() == 0) {
            File seedDataFile = new File(AbstractTest.TEST_RESOURCES_DIR, "seed/seed.dat");
            load(seedDataFile);
        }
    }
}
