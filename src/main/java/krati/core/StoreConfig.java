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
        this._properties.setProperty(StoreParams.PARAM_INITIAL_CAPACITY, initialCapacity + "");
        
        // Set the default segment factory
        this.setSegmentFactory(new MappedSegmentFactory());
        
        // Load properties from the default configuration file
        File file = new File(homeDir, CONFIG_PROPERTIES_FILE);
        if(file.exists()) {
            this.load(file);
        } else {
            this.store();
        }
    }
    
    public final File getHomeDir() {
        return _homeDir;
    }
    
    public final int getInitialCapacity() {
        return _initialCapacity;
    }
    
    public void list(PrintStream out) {
        _properties.list(out);
    }
    
    public void list(PrintWriter out) {
        _properties.list(out);
    }
    
    public void load() throws IOException {
        load(new File(getHomeDir(), CONFIG_PROPERTIES_FILE));
    }
    
    public void load(File propertiesFile) throws IOException {
        String paramName;
        String paramValue;
        
        Reader reader = new FileReader(propertiesFile);
        _properties.load(reader);
        reader.close();
        
        paramName = StoreParams.PARAM_INDEXES_CACHED;
        paramValue = _properties.getProperty(paramName);
        setIndexesCached(parseBoolean(paramName, paramValue, StoreParams.INDEXES_CACHED_DEFAULT));
        
        paramName = StoreParams.PARAM_INDEXES_BATCH_SIZE;
        paramValue = _properties.getProperty(paramName);
        setBatchSize(parseInt(paramName, paramValue, StoreParams.BATCH_SIZE_DEFAULT));
        
        paramName = StoreParams.PARAM_INDEXES_NUM_SYNC_BATCHES;
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
    }
    
    public void store() throws IOException {
        store(new File(getHomeDir(), CONFIG_PROPERTIES_FILE), null);
    }
    
    public void store(File propertiesFile, String comments) throws IOException {
        FileWriter writer = new FileWriter(propertiesFile);
        _properties.store(writer, comments);
        writer.close();
    }
    
    public void validate() throws InvalidStoreConfigException {
        if(getSegmentFactory() == null) {
            throw new InvalidStoreConfigException("Segment factory not found");
        }
        
        if(getBatchSize() < StoreParams.BATCH_SIZE_MIN) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_INDEXES_BATCH_SIZE + "=" + getBatchSize());
        }
        
        if(getNumSyncBatches() < StoreParams.NUM_SYNC_BATCHES_MIN) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_INDEXES_NUM_SYNC_BATCHES + "=" + getNumSyncBatches());
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
    
    public Set<String> propertyNames() {
        return _properties.stringPropertyNames();
    }
    
    public String getProperty(String pName) {
        return _properties.getProperty(pName);
    }
    
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
    
    public void setSegmentFactory(SegmentFactory segmentFactory) {
        if(segmentFactory == null) {
            throw new IllegalArgumentException("Invalid segmentFactory: " + segmentFactory);
        }
        
        this._segmentFactory = segmentFactory;
        this._properties.setProperty(PARAM_SEGMENT_FACTORY_CLASS, segmentFactory.getClass().getName());
    }
    
    public SegmentFactory getSegmentFactory() {
        return _segmentFactory;
    }
}
