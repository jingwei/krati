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

package krati.core.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryFactory;
import krati.core.array.entry.EntryValue;

/**
 * AbstractRecoverableArray
 * 
 * @author jwu
 * 
 * <p>
 * 05/09, 2011 - added abstract method getLogger <br/>
 * 
 */
abstract class AbstractRecoverableArray<V extends EntryValue> implements RecoverableArray<V> {
    /**
     * The length of this array.
     */
    protected int _length;
    
    /**
     * The home directory of this array.
     */
    protected File _directory;
    
    /**
     * The underlying array file (i.e. indexes.dat).
     */
    protected ArrayFile _arrayFile;
    
    /**
     * The factory for creating redo log entries.
     */
    protected EntryFactory<V> _entryFactory;
    
    /**
     * The manager for redo log entries.
     */
    protected ArrayEntryManager<V> _entryManager;
    
    /**
     * AbstractRecoverableArray.
     * 
     * @param length
     *            - the length of the array.
     * @param elemSize
     *            - the number of bytes per element.
     * @param batchSize
     *            - the number of updates per update batch.
     * @param numSyncBatches
     *            - the number of update batches required for updating the underlying indexes.
     * @param directory
     *            - the home directory to store the underlying indexes and redo log entry files.
     */
    protected AbstractRecoverableArray(int length, int elemSize, int batchSize, int numSyncBatches, File directory, EntryFactory<V> entryFactory) throws Exception {
        _length = length;
        _directory = directory;
        _entryFactory = entryFactory;
        _entryManager = new ArrayEntryManager<V>(this, numSyncBatches, batchSize);

        if (!_directory.exists()) {
            _directory.mkdirs();
        }

        File file = new File(_directory, "indexes.dat");
        _arrayFile = openArrayFile(file, length /* initial length */, elemSize);
        _length = _arrayFile.getArrayLength();

        init();

        getLogger().info("length:" + _length +
                        " elemSize:" + elemSize +
                        " batchSize:" + batchSize +
                        " numSyncBatches:" + numSyncBatches + 
                        " directory:" + directory.getAbsolutePath() +
                        " arrayFile:" + _arrayFile.getName());
    }
    
    /**
     * Loads data from the array file.
     */
    protected void init() throws IOException {
        try {
            long lwmScn = _arrayFile.getLwmScn();
            long hwmScn = _arrayFile.getHwmScn();
            if (hwmScn < lwmScn) {
                throw new IOException(_arrayFile.getAbsolutePath() + " is corrupted: lwmScn=" + lwmScn + " hwmScn=" + hwmScn);
            }

            // Initialize entry manager and process entry files on disk if any.
            _entryManager.init(lwmScn, hwmScn);

            // Load data from the array file on disk.
            loadArrayFileData();
        } catch (IOException e) {
            _entryManager.clear();
            getLogger().error(e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * Opens the {@link ArrayFile} with the specified initial length and the element size.
     * 
     * @param file          - the file backing up the array.
     * @param initialLength - the initial length of the array.
     * @param elementSize   - the number of bytes per element.
     * @return the instance of {@link ArrayFile}.
     * @throws IOException if this array file cannot be created for any reasons.
     */
    protected final ArrayFile openArrayFile(File file, int initialLength, int elementSize) throws IOException {
        boolean isNew = true;
        if (file.exists()) {
            isNew = false;
        }
        
        ArrayFile arrayFile = new ArrayFile(file, initialLength, elementSize);
        if (isNew) {
            initArrayFile();
        }
        
        return arrayFile;
    }
    
    /**
     * Subclasses need to initialize the array file.
     * 
     * @throws IOException
     */
    protected void initArrayFile() throws IOException {
        // Subclasses need to initialize ArrayFile
    }
    
    /**
     * Subclasses need to handle the loading of data in the array file.
     * 
     * @throws IOException
     */
    protected abstract void loadArrayFileData() throws IOException;
    
    /**
     * Subclasses need to provide the logger for logging purposes.
     */
    protected abstract Logger getLogger();
    
    /**
     * Gets the home directory of this array file.
     */
    public File getDirectory() {
        return _directory;
    }
    
    /**
     * Gets the factory of redo log entries.
     */
    public EntryFactory<V> getEntryFactory() {
        return _entryFactory;
    }
    
    /**
     * Gets the manger for redo log entries.
     */
    public ArrayEntryManager<V> getEntryManager() {
        return _entryManager;
    }
    
    @Override
    public boolean hasIndex(int index) {
        return (0 <= index && index < _length);
    }
    
    @Override
    public final int length() {
        return _length;
    }
    
    /**
     * Sync the array file with redo log entries.
     * The writer will be blocked until all redo log entries are applied.
     */
    @Override
    public void sync() throws IOException {
        _entryManager.sync();
        getLogger().info("array saved: length=" + length());
    }
    
    /**
     * Persists this array.
     */
    @Override
    public void persist() throws IOException {
        _entryManager.persist();
        getLogger().info("array persisted: length=" + length());
    }
    
    @Override
    public final long getHWMark() {
        return _entryManager.getHWMark();
    }
    
    @Override
    public final long getLWMark() {
        return _entryManager.getLWMark();
    }
    
    @Override
    public void updateArrayFile(List<Entry<V>> entryList) throws IOException {
        if(_arrayFile != null) {
            _arrayFile.update(entryList);
        }
    }
}
