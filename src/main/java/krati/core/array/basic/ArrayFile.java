package krati.core.array.basic;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;

import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryUtility;
import krati.core.array.entry.EntryValue;
import krati.io.BasicIO;
import krati.io.DataReader;
import krati.io.IOFactory;
import krati.io.IOType;
import krati.io.DataWriter;
import krati.util.Chronos;

/**
 * ArrayFile is not thread safe.
 *
 * <pre>
 * Version 0:
 * +--------------------------+
 * |Array header              |
 * |--------------------------|
 * |Storage Version    | long | 
 * |LWM Scn            | long |
 * |HWM Scn            | long |
 * |Array Length       | int  |
 * |Data Element Size  | int  |
 * |--------------------------|
 * |Array body starts at 1024 |
 * |                          |
 * +--------------------------+
 * </pre>
 * 
 * @author jwu
 * 
 * <p>
 * 05/09, 2011 - added support for java.io.Closeable
 * 06/24, 2011 - added setWaterMarks(lwmScn, hwmScn)
 */
public class ArrayFile implements Closeable {
  public static final long STORAGE_VERSION  = 0;
  public static final int ARRAY_HEADER_LENGTH = 1024;
  
  static final int VERSION_POSITION      = 0;
  static final int LWM_SCN_POSITION      = 8;
  static final int HWM_SCN_POSITION      = 16;
  static final int ARRAY_LENGTH_POSITION = 24;
  static final int ELEMENT_SIZE_POSITION = 28;
  static final long DATA_START_POSITION  = ARRAY_HEADER_LENGTH;
  
  static final Logger _log = Logger.getLogger(ArrayFile.class);

  private File _file;
  private IOType _type;
  private DataWriter _writer;
  
  // header information
  private long _version;
  private long _arrayLwmScn;
  private long _arrayHwmScn;
  private int  _arrayLength;  // array length (element count)
  private int  _elementSize;  // array element size in bytes
  
  /**
   * Creates a new ArrayFile based on a given length and element size.
   * 
   * @param file           the file on disk
   * @param initialLength  the initial length (number of elements) of array
   * @param elementSize    the size (number of bytes) of every array element
   * @throws IOException
   */
  public ArrayFile(File file, int initialLength, int elementSize) throws IOException {
      this(file, initialLength, elementSize, IOType.MAPPED);
  }
  
  /**
   * Creates a new ArrayFile based on a given length and element size.
   * 
   * @param file           the file on disk
   * @param initialLength  the initial length (number of elements) of array
   * @param elementSize    the size (number of bytes) of every array element
   * @param type           the I/O type
   * @throws IOException
   */
  public ArrayFile(File file, int initialLength, int elementSize, IOType type) throws IOException {
    boolean newFile = false;
    long initialFileLength = DATA_START_POSITION + ((long)initialLength * elementSize);
    
    if (!file.exists()) {
      if (!file.createNewFile()) {
        throw new IOException("Failed to create " + file.getAbsolutePath());
      }
      newFile = true;
    }
    
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    if (newFile) raf.setLength(initialFileLength);
    if (raf.length() < DATA_START_POSITION) {
      throw new IOException("Failed to open " + file.getAbsolutePath());
    }
    raf.close();
    
    this._file = file;
    this._type = type;
    this._writer = IOFactory.createDataWriter(_file, _type);;
    this._writer.open();
    
    if(newFile) {
      this._version = STORAGE_VERSION;
      this._arrayLwmScn = 0;
      this._arrayHwmScn = 0;
      this._arrayLength = initialLength;
      this._elementSize = elementSize;
      
      this.saveHeader();
    } else {
      this.loadHeader();
    }
    
    this.initCheck();
    
    _log.info(_file.getName() + " header: " + getHeader());
  }
  
  protected void initCheck() throws IOException {
    // Check storage version
    if (_version != STORAGE_VERSION) {
      throw new IOException("Invalid version in " + _file.getName() + ": "
              + _version + ", " + STORAGE_VERSION + " expected");
    }
    
    // Check array file header
    if(!checkHeader()) {
      throw new IOException("Invalid header in " + _file.getName() + ": " + getHeader());
    }
  }
  
  private void saveHeader() throws IOException {
    _writer.writeLong(VERSION_POSITION, _version);
    _writer.writeLong(LWM_SCN_POSITION, _arrayLwmScn);
    _writer.writeLong(HWM_SCN_POSITION, _arrayHwmScn);
    _writer.writeInt(ARRAY_LENGTH_POSITION, _arrayLength);
    _writer.writeInt(ELEMENT_SIZE_POSITION, _elementSize);
    _writer.flush();
  }
  
  private void loadHeader() throws IOException {
    ByteBuffer headerBuffer = ByteBuffer.allocate(ARRAY_HEADER_LENGTH);
    RandomAccessFile raf = new RandomAccessFile(_file, "rw");
    raf.getChannel().read(headerBuffer, 0);
    
    _version     = headerBuffer.getLong(VERSION_POSITION);
    _arrayLwmScn = headerBuffer.getLong(LWM_SCN_POSITION);
    _arrayHwmScn = headerBuffer.getLong(HWM_SCN_POSITION);
    _arrayLength = headerBuffer.getInt(ARRAY_LENGTH_POSITION);
    _elementSize = headerBuffer.getInt(ELEMENT_SIZE_POSITION);
    
    raf.close();
  }
  
  private boolean checkHeader() {
    // Array file is inconsistent if lwmScn is greater than hwmScn.
    if (_arrayHwmScn < _arrayLwmScn) return false;
    
    return true;
  }
  
  private String getHeader() {
    StringBuilder buf = new StringBuilder();
    
    buf.append("version=");
    buf.append(_version);
    buf.append(" lwmScn=");
    buf.append(_arrayLwmScn);
    buf.append(" hwmScn=");
    buf.append(_arrayHwmScn);
    buf.append(" arrayLength=");
    buf.append(_arrayLength);
    buf.append(" elementSize=");
    buf.append(_elementSize);
    
    return buf.toString();
  }
  
  public final String getName() {
    return _file.getName();
  }
  
  public final String getPath() {
    return _file.getPath();
  }
  
  public final String getAbsolutePath() {
    return _file.getAbsolutePath();
  }
  
  public final String getCanonicalPath() throws IOException {
    return _file.getCanonicalPath();
  }
  
  public final long getVersion() {
    return _version;
  }
  
  public final long getLwmScn() {
    return _arrayLwmScn;
  }
  
  public final long getHwmScn() {
    return _arrayHwmScn;
  }
  
  public final int getArrayLength() {
    return _arrayLength;
  }
  
  public final int getElementSize() {
    return _elementSize;
  }
  
  public final BasicIO getBasicIO() {
    return (BasicIO)_writer;
  }
  
  public void flush() throws IOException {
    _writer.flush();
  }
  
  public void force() throws IOException {
    _writer.force();
  }
  
  @Override
  public void close() throws IOException {
    _writer.close();
    _writer = null;
  }
  
  /**
   * Load data into a memory-based int array.
   * 
   * @throws IOException
   */
  public void load(MemoryIntArray intArray) throws IOException {
    if (!_file.exists() || _file.length() == 0) {
      return;
    }
    
    Chronos c = new Chronos();
    DataReader r = IOFactory.createDataReader(_file, _type);
    
    try {
      r.open();
      r.position(DATA_START_POSITION);
      
      for (int i = 0; i < _arrayLength; i++) {
        intArray.set(i, r.readInt());
      }
      
      _log.info(_file.getName() + " loaded in " + c.getElapsedTime());
    } finally {
      r.close();
    }
  }
  
  /**
   * Load data into a memory-based long array.
   * 
   * @throws IOException
   */
  public void load(MemoryLongArray longArray) throws IOException {
    if (!_file.exists() || _file.length() == 0) {
      return;
    }
    
    Chronos c = new Chronos();
    DataReader r = IOFactory.createDataReader(_file, _type);
    
    try {
      r.open();
      r.position(DATA_START_POSITION);
      
      for (int i = 0; i < _arrayLength; i++) {
        longArray.set(i, r.readLong());
      }
      
      _log.info(_file.getName() + " loaded in " + c.getElapsedTime());
    } finally {
      r.close();
    }
  }
  
  /**
   * Load data into a memory-based short array.
   * 
   * @throws IOException
   */
  public void load(MemoryShortArray shortArray) throws IOException {
    if (!_file.exists() || _file.length() == 0) {
      return;
    }
    
    Chronos c = new Chronos();
    DataReader r = IOFactory.createDataReader(_file, _type);
    
    try {
      r.open();
      r.position(DATA_START_POSITION);
      
      for (int i = 0; i < _arrayLength; i++) {
        shortArray.set(i, r.readShort());
      }
      
      _log.info(_file.getName() + " loaded in " + c.getElapsedTime());
    } finally {
      r.close();
    }
  }
  
  /**
   * Load the main array.
   * 
   * @return an int array
   * @throws IOException
   */
  public int[] loadIntArray() throws IOException {
    if (!_file.exists() || _file.length() == 0) {
      return null;
    }
    
    Chronos c = new Chronos();
    DataReader r = IOFactory.createDataReader(_file, _type);
    
    try {
      r.open();
      r.position(DATA_START_POSITION);
      
      int[] array = new int[_arrayLength];
      for (int i = 0; i < _arrayLength; i++) {
        array[i] = r.readInt();
      }
      
      _log.info(_file.getName() + " loaded in " + c.getElapsedTime());
      return array;
    } finally {
      r.close();
    }
  }
  
  /**
   * Load the main array.
   * 
   * @return a long array
   * @throws IOException
   */
  public long[] loadLongArray() throws IOException {
    if (!_file.exists() || _file.length() == 0) {
      return null;
    }
    
    Chronos c = new Chronos();
    DataReader r = IOFactory.createDataReader(_file, _type);
    
    try {
      r.open();
      r.position(DATA_START_POSITION);
      
      long[] array = new long[_arrayLength];
      for (int i = 0; i < _arrayLength; i++) {
        array[i] = r.readLong();
      }
      
      _log.info(_file.getName() + " loaded in " + c.getElapsedTime());
      return array;
    } finally {
      r.close();
    }
  }
  
  /**
   * Load the main array.
   * 
   * @return a short array
   * @throws IOException
   */
  public short[] loadShortArray() throws IOException {
    if (!_file.exists() || _file.length() == 0) {
      return null;
    }
    
    Chronos c = new Chronos();
    DataReader r = IOFactory.createDataReader(_file, _type);
    
    try {
      r.open();
      r.position(DATA_START_POSITION);
        
      short[] array = new short[_arrayLength];
      for (int i = 0; i < _arrayLength; i++) {
        array[i] = r.readShort();
      }
      
      _log.info(_file.getName() + " loaded in " + c.getElapsedTime());
      return array;
    } finally {
      r.close();
    }
  }
  
  protected long getPosition(int index) {
    return DATA_START_POSITION + ((long)index * _elementSize);
  }
  
  /**
   * Writes an int value at a specified index in the array.
   * 
   * This method does not update hwmScn and lwmScn in the array file.
   * 
   * @param index   an index in the array.
   * @param value   int value
   * @throws IOException
   */
  public void writeInt(int index, int value) throws IOException {
    _writer.writeInt(getPosition(index), value);
  }
  
  /**
   * Writes a long value at a specified index in the array.
   * 
   * This method does not update hwmScn and lwmScn in the array file.
   * 
   * @param index   an index in the array.
   * @param value   long value
   * @throws IOException
   */
  public void writeLong(int index, long value) throws IOException {
    _writer.writeLong(getPosition(index), value);
  }
  
  /**
   * Writes a short value at a specified index in the array.
   * 
   * This method does not update hwmScn and lwmScn in the array file.
   * 
   * @param index   an index in the array.
   * @param value   short value
   * @throws IOException
   */
  public void writeShort(int index, short value) throws IOException {
    _writer.writeShort(getPosition(index), value);
  }
  
  /**
   * Apply entries to the array file.
   * 
   * The method will flatten entry data and sort it by position.
   * So the array file can be updated sequentially to reduce disk seeking time.
   * 
   * This method updates hwmScn and lwmScn in the array file.
   * 
   * @param entryList
   * @throws IOException
   */
  public synchronized <T extends EntryValue> void update(List<Entry<T>> entryList)
  throws IOException {
    Chronos c = new Chronos();
    
    // Sort values by position in the array file
    T[] values = EntryUtility.sortEntriesToValues(entryList);
    if (values == null || values.length == 0) return;
    
    // Obtain maxScn
    long maxScn = 0;
    for (Entry<?> e : entryList) {
      maxScn = Math.max(e.getMaxScn(), maxScn);
    }
    
    // Write hwmScn
    _log.info("write hwmScn:" + maxScn);
    _writer.writeLong(HWM_SCN_POSITION, maxScn); 
    _writer.flush();
    
    // Write values
    for (T v : values) {
      v.updateArrayFile(_writer, getPosition(v.pos));
    }
    _writer.flush();
    
    // Write lwmScn
    _log.info("write lwmScn:" + maxScn);
    _writer.writeLong(LWM_SCN_POSITION, maxScn); 
    _writer.flush();
    
    _arrayLwmScn = maxScn;
    _arrayHwmScn = maxScn;
    
    _log.info(entryList.size() + " entries flushed to " + 
             _file.getAbsolutePath() + " in " + c.getElapsedTime());
  }

  protected void writeVersion(long value) throws IOException {
    _writer.writeLong(VERSION_POSITION, value);
    _version = value;
  }
  
  protected void writeLwmScn(long value) throws IOException {
    _writer.writeLong(LWM_SCN_POSITION, value);
    _arrayLwmScn = value;
  }
  
  protected void writeHwmScn(long value) throws IOException {
    _writer.writeLong(HWM_SCN_POSITION, value);
    _arrayHwmScn = value;
  }
  
  protected void writeArrayLength(int value) throws IOException {
    _writer.writeInt(ARRAY_LENGTH_POSITION, value);
    _arrayLength = value;
  }
  
  protected void writeElementSize(int value) throws IOException {
    _writer.writeInt(ELEMENT_SIZE_POSITION, value);
    _elementSize = value;
  }
  
  public void setWaterMarks(long lwmScn, long hwmScn) throws IOException {
      if(lwmScn <= hwmScn) {
          writeHwmScn(hwmScn);
          _writer.flush();
          writeLwmScn(lwmScn);
          _writer.flush();
      } else {
          throw new IOException("Invalid water marks: lwmScn=" + lwmScn + " hwmScn=" + hwmScn);
      }
  }
  
  public synchronized void reset(MemoryIntArray intArray) throws IOException {
      _writer.flush();
      _writer.position(DATA_START_POSITION);
      for(int i = 0, cnt = intArray.length(); i < cnt; i++) {
          _writer.writeInt(intArray.get(i));
      }
      _writer.flush();
  }
  
  public synchronized void reset(MemoryIntArray intArray, long maxScn) throws IOException {
      reset(intArray);
      
      _log.info("update hwmScn and lwmScn:" + maxScn);
      writeHwmScn(maxScn);
      writeLwmScn(maxScn);
      flush();
  }
  
  public synchronized void reset(MemoryLongArray longArray) throws IOException {
      _writer.flush();
      _writer.position(DATA_START_POSITION);
      for(int i = 0, cnt = longArray.length(); i < cnt; i++) {
          _writer.writeLong(longArray.get(i));
      }
      _writer.flush();
  }
  
  public synchronized void reset(MemoryLongArray longArray, long maxScn) throws IOException {
      reset(longArray);
      
      _log.info("update hwmScn and lwmScn:" + maxScn);
      writeHwmScn(maxScn);
      writeLwmScn(maxScn);
      flush();
  }
  
  public synchronized void reset(MemoryShortArray shortArray) throws IOException {
      _writer.flush();
      _writer.position(DATA_START_POSITION);
      for(int i = 0, cnt = shortArray.length(); i < cnt; i++) {
          _writer.writeShort(shortArray.get(i));
      }
      _writer.flush();
  }
  
  public synchronized void reset(MemoryShortArray shortArray, long maxScn) throws IOException {
      reset(shortArray);
      
      _log.info("update hwmScn and lwmScn:" + maxScn);
      writeHwmScn(maxScn);
      writeLwmScn(maxScn);
      flush();
  }
  
  public synchronized void reset(int[] intArray) throws IOException {
      _writer.flush();
      _writer.position(DATA_START_POSITION);
      for(int i = 0; i < intArray.length; i++) {
          _writer.writeInt(intArray[i]);
      }
      _writer.flush();
  }
  
  public synchronized void reset(int[] intArray, long maxScn) throws IOException {
      reset(intArray);
      
      _log.info("update hwmScn and lwmScn:" + maxScn);
      writeHwmScn(maxScn);
      writeLwmScn(maxScn);
      flush();
  }
  
  public synchronized void reset(long[] longArray) throws IOException {
      _writer.flush();
      _writer.position(DATA_START_POSITION);
      for(int i = 0; i < longArray.length; i++) {
          _writer.writeLong(longArray[i]);
      }
      _writer.flush();
  }
  
  public synchronized void reset(long[] longArray, long maxScn) throws IOException {
      reset(longArray);
      
      _log.info("update hwmScn and lwmScn:" + maxScn);
      writeHwmScn(maxScn);
      writeLwmScn(maxScn);
      flush();
  }
  
  public synchronized void reset(short[] shortArray) throws IOException {
      _writer.flush();
      _writer.position(DATA_START_POSITION);
      for(int i = 0; i < shortArray.length; i++) {
          _writer.writeShort(shortArray[i]);
      }
      _writer.flush();
  }
  
  public synchronized void reset(short[] shortArray, long maxScn) throws IOException {
      reset(shortArray);
      
      _log.info("update hwmScn and lwmScn:" + maxScn);
      writeHwmScn(maxScn);
      writeLwmScn(maxScn);
      flush();
  }
  
  public synchronized void resetAll(long value) throws IOException {
      if(_elementSize != 8) {
          throw new IOException("Operation aborted: elementSize=" + _elementSize);
      }
      
      _writer.flush();
      _writer.position(DATA_START_POSITION);
      for(int i = 0; i < this._arrayLength; i++) {
          _writer.writeLong(value);
      }
      _writer.flush();
  }
  
  public synchronized void resetAll(long value, long maxScn) throws IOException {
      resetAll(value);
      
      _log.info("update hwmScn and lwmScn:" + maxScn);
      writeHwmScn(maxScn);
      writeLwmScn(maxScn);
      flush();
  }
  
  public synchronized void setArrayLength(int arrayLength, File renameToFile) throws IOException {
      if(arrayLength < 0) {
          throw new IOException("Invalid array length: " + arrayLength);
      }
      
      if(this._arrayLength == arrayLength) return;
      
      // Flush all the changes.
      this.flush();
      
      // Change the file length.
      long fileLength = DATA_START_POSITION + ((long)arrayLength * _elementSize);
      RandomAccessFile raf = new RandomAccessFile(_file, "rw");
      try {
          raf.setLength(fileLength);
      } catch(IOException e) {
          _log.error("failed to setArrayLength " + arrayLength);
          throw e;
      } finally {
          raf.close();
      }
      
      // Write the new array length.
      writeArrayLength(arrayLength);
      this.flush();
      
      if(renameToFile != null) {
          if(_file.renameTo(renameToFile)) {
              _writer.close();
              _file = renameToFile;
              _writer = IOFactory.createDataWriter(_file, _type);
              _writer.open();
              return;
          } else {
              _log.warn("Failed to rename " + _file.getAbsolutePath() + " to " + renameToFile.getAbsolutePath());
          }
      }
      
      _writer.close();
      _writer = IOFactory.createDataWriter(_file, _type);
      _writer.open();
  }
}
