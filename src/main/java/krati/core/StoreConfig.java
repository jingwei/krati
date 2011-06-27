package krati.core;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Set;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

import org.apache.log4j.Logger;

/**
 * StoreConfig
 * 
 * @author jwu
 * 06/22, 2011
 * 
 * <p>
 * 06/25, 2011 - Added method validate()
 */
public class StoreConfig extends StoreParams {
    public final static String CONFIG_PROPERTIES_FILE = "config.properties";
    private final static Logger _logger = Logger.getLogger(StoreConfig.class);
    private final File _homeDir;
    private final int _initialCapacity;
    private SegmentFactory _segmentFactory = null;
    private HashFunction<byte[]> _hashFunction = null;
    
    /**
     * Creates the configuration of a target store
     * 
     * @param homeDir         - the home directory of the target store
     * @param initialCapacity - the initial capacity of the target store
     * @throws IOException if the store configuration file cannot be created.
     */
    public StoreConfig(File homeDir, int initialCapacity) throws IOException {
        if(!homeDir.exists()) {
            homeDir.mkdirs();
        }
        
        if(homeDir.isFile()) {
            throw new IOException("Invalid homeDir: " + homeDir.getAbsolutePath()); 
        }
        
        if(initialCapacity < 1) {
            throw new IllegalArgumentException("Invalid initialCapacity: " + initialCapacity);
        }
        
        this._homeDir = homeDir;
        this._initialCapacity = initialCapacity;
        this._properties.setProperty(StoreParams.PARAM_INITIAL_CAPACITY, _initialCapacity + "");
        
        // Set the default segment factory
        this.setSegmentFactory(new MappedSegmentFactory());
        
        // Set the default hash function
        this.setHashFunction(new FnvHashFunction());
        
        // Load properties from the default configuration file
        File file = new File(homeDir, CONFIG_PROPERTIES_FILE);
        if(file.exists()) {
            this.load(file);
            this.validate();
        } else {
            this.save();
        }
    }
        
    /**
     * Creates the configuration of a target array store partition.
     * 
     * @param homeDir         - the home directory of the target array store
     * @param partitionStart  - the start of the target array store partition
     * @param partitionCount  - the count of the target array store partition
     * @throws IOException if the store configuration file cannot be created.
     */
    StoreConfig(File homeDir, int partitionStart, int partitionCount) throws IOException {
        if(!homeDir.exists()) {
            homeDir.mkdirs();
        }
        
        if(homeDir.isFile()) {
            throw new IOException("Invalid homeDir: " + homeDir.getAbsolutePath()); 
        }
        
        if(partitionStart < 0) {
            throw new IllegalArgumentException("Invalid partitionStart: " + partitionStart);
        }
        
        if(partitionCount < 1) {
            throw new IllegalArgumentException("Invalid partitionCount: " + partitionCount);
        }
        
        long partitionEnd = (long)partitionStart + (long)partitionCount;
        if(partitionEnd > Integer.MAX_VALUE) {
            throw new InvalidStoreConfigException("Invalid partition: start=" + partitionStart + " count=" + partitionCount);
        }
        
        this._homeDir = homeDir;
        this._initialCapacity = partitionCount;
        this._properties.setProperty(StoreParams.PARAM_INITIAL_CAPACITY, _initialCapacity + "");
        this._properties.setProperty(StoreParams.PARAM_PARTITION_START, partitionStart + "");
        this._properties.setProperty(StoreParams.PARAM_PARTITION_COUNT, partitionCount + "");
        
        // Set the default segment factory
        this.setSegmentFactory(new MappedSegmentFactory());
        
        // Set the default hash function
        this.setHashFunction(new FnvHashFunction());
        
        // Load properties from the default configuration file
        File file = new File(homeDir, CONFIG_PROPERTIES_FILE);
        if(file.exists()) {
            this.load(file);
            this.validate();
        } else {
            this.save();
        }
    }
    
    /**
     * @return the home directory of the target store.
     */
    public final File getHomeDir() {
        return _homeDir;
    }
    
    /**
     * @return the initial capacity of the target store.
     */
    public final int getInitialCapacity() {
        return _initialCapacity;
    }
    
    /**
     * Lists store configuration properties to a print stream.
     * 
     * @param out
     */
    public void list(PrintStream out) {
        _properties.list(out);
    }
    
    /**
     * Lists store configuration properties to a print writer.
     * 
     * @param out
     */
    public void list(PrintWriter out) {
        _properties.list(out);
    }
    
    /**
     * Loads configuration from the default file <code>config.properties</code>.
     * 
     * @throws IOException
     */
    public void load() throws IOException {
        load(new File(getHomeDir(), CONFIG_PROPERTIES_FILE));
    }
    
    /**
     * Loads configuration from a properties file.
     * 
     * @param propertiesFile - a configuration properties file
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void load(File propertiesFile) throws IOException {
        String paramName;
        String paramValue;
        
        Reader reader = new FileReader(propertiesFile);
        _properties.load(reader);
        reader.close();
        
        paramName = StoreParams.PARAM_INDEXES_CACHED;
        paramValue = _properties.getProperty(paramName);
        setIndexesCached(parseBoolean(paramName, paramValue, StoreParams.INDEXES_CACHED_DEFAULT));
        
        paramName = StoreParams.PARAM_BATCH_SIZE;
        paramValue = _properties.getProperty(paramName);
        setBatchSize(parseInt(paramName, paramValue, StoreParams.BATCH_SIZE_DEFAULT));
        
        paramName = StoreParams.PARAM_NUM_SYNC_BATCHES;
        paramValue = _properties.getProperty(paramName);
        setNumSyncBatches(parseInt(paramName, paramValue, StoreParams.NUM_SYNC_BATCHES_DEFAULT));
        
        paramName = StoreParams.PARAM_SEGMENT_FILE_SIZE_MB;
        paramValue = _properties.getProperty(paramName);
        setSegmentFileSizeMB(parseInt(paramName, paramValue, StoreParams.SEGMENT_FILE_SIZE_MB_DEFAULT));
        
        paramName = StoreParams.PARAM_SEGMENT_COMPACT_FACTOR;
        paramValue = _properties.getProperty(paramName);
        setSegmentCompactFactor(parseDouble(paramName, paramValue, StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT));
        
        paramName = StoreParams.PARAM_HASH_LOAD_FACTOR;
        paramValue = _properties.getProperty(paramName);
        setHashLoadFactor(parseDouble(paramName, paramValue, StoreParams.HASH_LOAD_FACTOR_DEFAULT));
        
        // Create _segmentFactory
        paramName = StoreParams.PARAM_SEGMENT_FACTORY_CLASS;
        paramValue = _properties.getProperty(paramName);
        SegmentFactory segmentFactory = null;
        if(paramValue != null) {
            try {
                segmentFactory = Class.forName(paramValue).asSubclass(SegmentFactory.class).newInstance();
            } catch(Exception e) {
                _logger.warn("Invalid SegmentFactory class: " + paramValue);
            }
        }
        if(segmentFactory == null) {
            segmentFactory = new MappedSegmentFactory();
        }
        setSegmentFactory(segmentFactory);
        
        // Create _hashFunction
        paramName = StoreParams.PARAM_HASH_FUNCTION_CLASS;
        paramValue = _properties.getProperty(paramName);
        HashFunction<byte[]> hashFunction = null;
        if(paramValue != null) {
            try {
                hashFunction = (HashFunction<byte[]>)Class.forName(paramValue).newInstance();
            } catch(Exception e) {
                _logger.warn("Invalid HashFunction<byte[]> class: " + paramValue);
            }
        }
        if(hashFunction == null) {
            hashFunction = new FnvHashFunction();
        }
        setHashFunction(hashFunction);
    }
    
    /**
     * Saves configuration to the default file <code>config.properties</code>
     * 
     * @throws IOException
     */
    public void save() throws IOException {
        save(new File(getHomeDir(), CONFIG_PROPERTIES_FILE), null);
    }
    
    /**
     * Saves configuration to a properties file in a format suitable for using {{@link #load(File)}.
     * 
     * @param propertiesFile - a configuration properties file
     * @param comments       - a description of the configuration
     * @throws IOException
     */
    public void save(File propertiesFile, String comments) throws IOException {
        FileWriter writer = new FileWriter(propertiesFile);
        _properties.store(writer, comments);
        writer.close();
    }
    
    /**
     * Checks the validity of this StoreConfig.
     *  
     * @throws InvalidStoreConfigException if any store parameter is found invalid. 
     */
    public void validate() throws InvalidStoreConfigException {
        if(getSegmentFactory() == null) {
            throw new InvalidStoreConfigException("Segment factory not found");
        }
        
        if(getHashFunction() == null) {
            throw new InvalidStoreConfigException("Store hash function not found");
        }
        
        if(getBatchSize() < StoreParams.BATCH_SIZE_MIN) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_BATCH_SIZE + "=" + getBatchSize());
        }
        
        if(getNumSyncBatches() < StoreParams.NUM_SYNC_BATCHES_MIN) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_NUM_SYNC_BATCHES + "=" + getNumSyncBatches());
        }
        
        if(getHashLoadFactor() < StoreParams.HASH_LOAD_FACTOR_MIN || getHashLoadFactor() > StoreParams.HASH_LOAD_FACTOR_MAX) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_HASH_LOAD_FACTOR + "=" + getHashLoadFactor());
        }
        
        if(getSegmentFileSizeMB() < StoreParams.SEGMENT_FILE_SIZE_MB_MIN || getSegmentFileSizeMB() > StoreParams.SEGMENT_FILE_SIZE_MB_MAX) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_SEGMENT_FILE_SIZE_MB + "=" + getSegmentFileSizeMB());
        }
        
        if(getSegmentCompactFactor() < StoreParams.SEGMENT_COMPACT_FACTOR_MIN || getSegmentCompactFactor() > StoreParams.SEGMENT_COMPACT_FACTOR_MAX) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_SEGMENT_COMPACT_FACTOR + "=" + getSegmentCompactFactor());
        }
    }
    
    /**
     * @return a set of string property keys of this StoreConfig. 
     */
    public Set<String> propertyNames() {
        return _properties.stringPropertyNames();
    }
    
    /**
     * Gets a property value via a string property name.
     * 
     * @param pName - a property name
     * @return a string property value.
     */
    public String getProperty(String pName) {
        return _properties.getProperty(pName);
    }
    
    /**
     * Sets a store configuration property via its name and value.
     *  
     * @param pName  - a property name
     * @param pValue - a property value
     * @return <code>true</code> if the property is set successfully.
     */
    public boolean setProperty(String pName, String pValue) {
        if(pName == null) return false;
        _properties.setProperty(pName, pValue);
        return true;
    }
    
    static int parseInt(String pName, String pValue, int defaultValue) {
        try {
            if(pValue != null) {
                return Integer.parseInt(pValue);
            }
        } catch(Exception e) {
            _logger.error("failed to parse " + pName + "=" + pValue);
        }
        return defaultValue;
    }
    
    static double parseDouble(String pName, String pValue, double defaultValue) {
        try {
            if(pValue != null) {
                return Double.parseDouble(pValue);
            }
        } catch(Exception e) {
            _logger.error("failed to parse " + pName + "=" + pValue);
        }
        return defaultValue;
    }
    
    static boolean parseBoolean(String pName, String pValue, boolean defaultValue) {
        try {
            if(pValue != null) {
                return Boolean.parseBoolean(pValue);
            }
        } catch(Exception e) {
            _logger.error("failed to parse " + pName + "=" + pValue);
        }
        
        return defaultValue;
    }
    
    /**
     * Sets the segment factory of the target store.
     * 
     * @param segmentFactory
     */
    public void setSegmentFactory(SegmentFactory segmentFactory) {
        if(segmentFactory == null) {
            throw new IllegalArgumentException("Invalid segmentFactory: " + segmentFactory);
        }
        
        this._segmentFactory = segmentFactory;
        this._properties.setProperty(PARAM_SEGMENT_FACTORY_CLASS, segmentFactory.getClass().getName());
    }
    
    /**
     * Gets the segment factory of the target store.
     */
    public SegmentFactory getSegmentFactory() {
        return _segmentFactory;
    }
    
    /**
     * Sets the hash function of the target {#link krati.store.DataStore DataStore}.
     * 
     * @param hashFunction
     */
    public void setHashFunction(HashFunction<byte[]> hashFunction) {
        if(hashFunction == null) {
            throw new IllegalArgumentException("Invalid hashFunction: " + hashFunction);
        }
        
        this._hashFunction = hashFunction;
        this._properties.setProperty(PARAM_HASH_FUNCTION_CLASS, hashFunction.getClass().getName());
    }
    
    /**
     * Gets the hash function of the target {#link krati.store.DataStore DataStore}.
     */
    public HashFunction<byte[]> getHashFunction() {
        return _hashFunction;
    }
}
