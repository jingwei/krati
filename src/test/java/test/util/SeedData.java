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
