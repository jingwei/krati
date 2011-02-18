package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * AbstractSeedTest
 * 
 * @author jwu
 *
 */
public abstract class AbstractSeedTest extends AbstractTest {
    protected static List<String> _lineSeedData = new ArrayList<String>(10000);
    
    protected static void loadSeedData(File dataFile) throws IOException {
        String line;
        FileReader reader = new FileReader(dataFile);
        BufferedReader in = new BufferedReader(reader);
        
        while((line = in.readLine()) != null) {
            _lineSeedData.add(line);
        }
        
        in.close();
        reader.close();
    }
    
    protected static void loadSeedData() throws IOException {
        if(_lineSeedData.size() == 0) {
            File seedDataFile = new File(TEST_RESOURCES_DIR, "seed/seed.dat");
            loadSeedData(seedDataFile);
        }
    }
    
    protected AbstractSeedTest(String name) {
        super(name);
    }
}
