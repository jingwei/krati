package test.bdb;

import java.io.File;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * SimpleDBEnv
 * 
 * @author jwu
 * 
 */
public class SimpleDBEnv {
    private Environment env;
    private Database simpleDB;
    private ClassCatalog catalog;
    private StoredMap<String, String> map;
    
    // Our constructor does nothing
    public SimpleDBEnv() {}

    // The setup() method opens all our databases and the environment for us.
    @SuppressWarnings("deprecation")
    public void setup(File envHome, boolean readOnly) throws DatabaseException {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setTxnNoSync(true);
        
        DatabaseConfig dbConfig = new DatabaseConfig();
        
        // If the environment is read-only, then
        // make the databases read-only too.
        envConfig.setReadOnly(readOnly);
        dbConfig.setReadOnly(readOnly);

        // If the environment is opened for write, then we want to be
        // able to create the environment and databases if
        // they do not exist.
        envConfig.setAllowCreate(!readOnly);
        dbConfig.setAllowCreate(!readOnly);

        // Allow transactions if we are writing to the database
        envConfig.setTransactional(!readOnly);
        dbConfig.setTransactional(!readOnly);

        // Open the environment
        env = new Environment(envHome, envConfig);

        // Now open, or create and open, our databases
        // Open the vendors and inventory databases
        simpleDB = env.openDatabase(null, "SimpleDB", dbConfig);
        
        catalog = new StoredClassCatalog(simpleDB);
        SerialBinding<String> keyBinding =
          new SerialBinding<String>(catalog, String.class);

        SerialBinding<String> valBinding =
            new SerialBinding<String>(catalog, String.class);
        
        // Create a map view of the database
        map = new StoredMap<String, String>(simpleDB, keyBinding, valBinding, true);
    }
    
    // Needed for things like beginning transactions
    public Environment getEnv() {
        return env;
    }
    
    public Database getSimpleDB() {
        return simpleDB;
    }
    
    public StoredMap<String, String> getMap() {
        return map;
    }
    
    //Close the environment
    public void close() {
        if (env != null) {
            try {
                simpleDB.close();
                env.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing MyDbEnv: " + dbe.toString());
               System.exit(-1);
            }
        }
    }
}
