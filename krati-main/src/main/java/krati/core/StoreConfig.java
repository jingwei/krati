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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Properties;
import java.util.Set;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataHandler;
import krati.store.DefaultDataSetHandler;
import krati.store.DefaultDataStoreHandler;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

import org.apache.log4j.Logger;

/**
 * StoreConfig provides a simple means for configuring a store
 * with many default parameters defined in {@link StoreParams}.
 * 
 * @author jwu
 * @since 06/22, 2011
 * 
 * <p>
 * 06/25, 2011 - Added method validate() <br/>
 * 10/01, 2011 - Added static method newInstance(File) <br/>
 */
public class StoreConfig extends StoreParams {
    private final static Logger _logger = Logger.getLogger(StoreConfig.class);
    private final File _homeDir;
    private final int _initialCapacity;
    private DataHandler _dataHandler = null;
    private SegmentFactory _segmentFactory = null;
    private HashFunction<byte[]> _hashFunction = null;
    
    /**
     * The store configuration properties file: <code>config.properties</code>.
     */
    public final static String CONFIG_PROPERTIES_FILE = "config.properties";
    
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
        
        // Create _dataHandler
        paramName = StoreParams.PARAM_DATA_HANDLER_CLASS;
        paramValue = _properties.getProperty(paramName);
        DataHandler dataHandler = null;
        if(paramValue != null) {
            try {
                dataHandler = (DataHandler)Class.forName(paramValue).newInstance();
            } catch(Exception e) {
                _logger.warn("Invalid DataHandler class: " + paramValue);
            }
        }
        if(dataHandler != null) {
            setDataHandler(dataHandler);
        }
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
     * @param pName - the property name
     * @return a string property value.
     */
    public String getProperty(String pName) {
        return _properties.getProperty(pName);
    }
    
    /**
     * Sets a store configuration property via its name and value.
     *  
     * @param pName  - the property name
     * @param pValue - the property value
     * @return <code>true</code> if the property is set successfully.
     */
    public boolean setProperty(String pName, String pValue) {
        if(pName == null) return false;
        _properties.setProperty(pName, pValue);
        return true;
    }
    
    /**
     * Sets an integer property via a string property name.
     */ 
    public boolean setInt(String pName, int pValue) {
        if(pName == null) return false;
        _properties.setProperty(pName, pValue + "");
        return true;
    }
    
    /**
     * Sets a float property via a string property name.
     */ 
    public boolean setFloat(String pName, float pValue) {
        if(pName == null) return false;
        _properties.setProperty(pName, pValue + "");
        return true;
    }
    
    /**
     * Sets a double property via a string property name.
     */ 
    public boolean setDouble(String pName, double pValue) {
        if(pName == null) return false;
        _properties.setProperty(pName, pValue + "");
        return true;
    }
    
    /**
     * Sets a double property via a string property name.
     */ 
    public boolean setBoolean(String pName, boolean pValue) {
        if(pName == null) return false;
        _properties.setProperty(pName, pValue + "");
        return true;
    }
    
    /**
     * Gets an integer property via a string property name.
     * 
     * @param pName        - the property name
     * @param defaultValue - the default property value
     * @return
     */
    public int getInt(String pName, int defaultValue) {
        String pValue = _properties.getProperty(pName);
        return parseInt(pName, pValue, defaultValue);
    }
    
    /**
     * Gets a float property via a string property name.
     * 
     * @param pName        - the property name
     * @param defaultValue - the default property value
     * @return
     */
    public float getFloat(String pName, float defaultValue) {
        String pValue = _properties.getProperty(pName);
        return parseFloat(pName, pValue, defaultValue);
    }
    
    /**
     * Gets a double property via a string property name.
     * 
     * @param pName        - the property name
     * @param defaultValue - the default property value
     * @return
     */
    public double getDouble(String pName, double defaultValue) {
        String pValue = _properties.getProperty(pName);
        return parseDouble(pName, pValue, defaultValue);
    }
    
    /**
     * Gets a boolean property via a string property name.
     * 
     * @param pName        - the property name
     * @param defaultValue - the default property value
     * @return
     */
    public boolean getBoolean(String pName, boolean defaultValue) {
        String pValue = _properties.getProperty(pName);
        return parseBoolean(pName, pValue, defaultValue);
    }
    
    /**
     * Gets a class property via a string property name.
     * 
     * @param pName        - the property name
     * @param defaultValue - the default property value
     * @return
     */
    public Class<?> getClass(String pName, Class<?> defaultValue) {
        String pValue = _properties.getProperty(pName);
        return parseClass(pName, pValue, defaultValue);
    }
    
    static int parseInt(String pName, String pValue, int defaultValue) {
        try {
            if(pValue != null) {
                return Integer.parseInt(pValue);
            }
        } catch(Exception e) {
            _logger.warn("failed to parse " + pName + "=" + pValue + " default=" + defaultValue);
        }
        return defaultValue;
    }
    
    static float parseFloat(String pName, String pValue, float defaultValue) {
        try {
            if(pValue != null) {
                return Float.parseFloat(pValue);
            }
        } catch(Exception e) {
            _logger.warn("failed to parse " + pName + "=" + pValue + " default=" + defaultValue);
        }
        return defaultValue;
    }
    
    static double parseDouble(String pName, String pValue, double defaultValue) {
        try {
            if(pValue != null) {
                return Double.parseDouble(pValue);
            }
        } catch(Exception e) {
            _logger.warn("failed to parse " + pName + "=" + pValue + " default=" + defaultValue);
        }
        return defaultValue;
    }
    
    static boolean parseBoolean(String pName, String pValue, boolean defaultValue) {
        try {
            if(pValue != null) {
                return Boolean.parseBoolean(pValue);
            }
        } catch(Exception e) {
            _logger.warn("failed to parse " + pName + "=" + pValue + " default=" + defaultValue);
        }
        
        return defaultValue;
    }
    
    static Class<?> parseClass(String pName, String pValue, Class<?> defaultValue) {
        try {
            if(pValue != null) {
                return Class.forName(pValue);
            }
        } catch(Exception e) {
            _logger.warn("failed to parse " + pName + "=" + pValue  + " default=" + defaultValue);
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
    
    /**
     * Sets the data handler of the target {#link krati.store.DataStore DataStore}.
     * 
     * @param dataHandler
     */
    public void setDataHandler(DataHandler dataHandler) {
        this._dataHandler = dataHandler;
        if(dataHandler == null) {
            _properties.remove(PARAM_DATA_HANDLER_CLASS);
        } else {
            if (dataHandler.getClass() != DefaultDataSetHandler.class &&
                dataHandler.getClass() != DefaultDataStoreHandler.class) {
                _properties.setProperty(PARAM_DATA_HANDLER_CLASS, dataHandler.getClass().getName());
            }
        }
    }
    
    /**
     * Gets the data handler of the target {#link krati.store.DataStore DataStore}.
     */
    public DataHandler getDataHandler() {
        return _dataHandler;
    }
    
    /**
     * Creates a new instance of StoreConfig.
     * 
     * @param filePath - the <code>config.properties</code> file or the directory containing <code>config.properties</code> 
     * @return a new instance of StoreConfig
     * @throws IOException
     * @throws InvalidStoreConfigException
     */
    public static StoreConfig newInstance(File filePath) throws IOException {
        File homeDir;
        Properties p = new Properties();
        if(filePath.exists()) {
            if(filePath.isDirectory()) {
                homeDir = filePath;
                File propertiesFile = new File(filePath, CONFIG_PROPERTIES_FILE);
                if(propertiesFile.exists()) {
                    FileReader reader = new FileReader(propertiesFile);
                    p.load(reader);
                    reader.close();
                } else {
                    throw new FileNotFoundException(propertiesFile.toString());
                }
            } else {
                homeDir = filePath.getParentFile();
                FileReader reader = new FileReader(filePath);
                p.load(reader);
                reader.close();
            }
        } else {
            throw new FileNotFoundException(filePath.toString());
        }
        
        int initialCapacity = -1;
        String initialCapacityStr = p.getProperty(StoreParams.PARAM_INITIAL_CAPACITY);
        if(initialCapacityStr == null) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_INITIAL_CAPACITY + " not found");
        } else if((initialCapacity = parseInt(StoreParams.PARAM_INITIAL_CAPACITY, initialCapacityStr, -1)) < 1) {
            throw new InvalidStoreConfigException(StoreParams.PARAM_INITIAL_CAPACITY + "=" + initialCapacityStr);
        }
        
        int partitionStart = -1;
        int partitionCount = -1;
        String partitionStartStr = p.getProperty(StoreParams.PARAM_PARTITION_START);
        String partitionCountStr = p.getProperty(StoreParams.PARAM_PARTITION_COUNT);
        if(partitionStartStr != null && partitionCountStr != null) {
            if((partitionStart = parseInt(StoreParams.PARAM_PARTITION_START, partitionStartStr, -1)) < 0) {
                throw new InvalidStoreConfigException(StoreParams.PARAM_PARTITION_START + "=" + partitionStartStr);
            }
            
            if((partitionCount = parseInt(StoreParams.PARAM_PARTITION_COUNT, partitionCountStr, -1)) < 1) {
                throw new InvalidStoreConfigException(StoreParams.PARAM_PARTITION_COUNT + "=" + partitionCountStr);
            }
        } else {
            return new StoreConfig(homeDir, initialCapacity);
        }
        
        return new StorePartitionConfig(homeDir, partitionStart, partitionCount);
    }
}
