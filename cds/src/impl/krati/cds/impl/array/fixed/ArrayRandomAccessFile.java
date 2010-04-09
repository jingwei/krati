package krati.cds.impl.array.fixed;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.apache.log4j.Logger;

import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryUtility;
import krati.cds.impl.array.entry.EntryValue;
import krati.io.ChannelReader;
import krati.io.ChannelWriter;
import krati.util.Chronos;

/**
 * Array in a RandomAccessFile form. This class is not thread safe.
 * 
 * Once the _arrayLength is set, the array file can't be changed.
 *
 * ArrayFile structure:
 * +--------------------------+
 * |Header                    |
 * |--------------------------|
 * |Array File Version | long | 
 * |Max SCN            | long |
 * |Copy SCN           | long |
 * |Array Length       | int  |
 * |Data Element Size  | int  |
 * |--------------------------|
 * | Array data begins at 1024|
 * |                          |
 * +--------------------------+
 * 
 */
public class ArrayRandomAccessFile
{
  public static final long ARRAY_FILE_VERSION = 0;
  
  private static final int MAX_SCN_POS      = 1;
  private static final int COPY_SCN_POS     = 2;
  private static final int ARRAY_LENGTH_POS = 3;
  private static final int ELEMENT_SIZE_POS = 4;
  private static final long DATA_START_POS  = 1024;
  
  private static final Logger log = Logger.getLogger(ArrayRandomAccessFile.class);
  
  private RandomAccessFile _raf;
  private final File       _file;
  private final int        _arrayLength;  // array length (e.g. member count)
  private final int        _elementSize;  // element size in bytes
  
  public ArrayRandomAccessFile(File file, int arrayLength, int elementSize) throws IOException
  {
    this._file = file;
    this._arrayLength = arrayLength;
    this._elementSize = elementSize;
    long fileLength = DATA_START_POS + (elementSize * arrayLength);
    
    this._raf = new RandomAccessFile(file, "rw");
    if(_raf.length() == 0)
    {
      // allocate the total size of the file
      _raf.setLength(fileLength);
      _raf.writeLong(ARRAY_FILE_VERSION); // write version
      writeMaxSCN(0);
      writeCopySCN(0);
      writeArrayLength(arrayLength);
      writeElementSize(elementSize);
    }
    assert _raf.length() == fileLength;
    
    if (_raf.length() > 0)
    {
      long fileVersion = _raf.readLong();
      if (fileVersion != ARRAY_FILE_VERSION)
      {
        throw new RuntimeException("Wrong version " + fileVersion + " read from array file "
                                   + file.getAbsolutePath() + ". Version " + ARRAY_FILE_VERSION + " expected.");
      }
    }
  }
  
  public String getName()
  {
    return _file.getName();
  }
  
  public String getPath()
  {
    return _file.getPath();
  }
  
  public String getAbsolutePath()
  {
    return _file.getAbsolutePath();
  }
  
  public String getCanonicalPath() throws IOException
  {
    return _file.getCanonicalPath();
  }
  
  public void sync() throws IOException
  {
    _raf.getFD().sync();
  }
  
  public void close() throws IOException
  {
    _raf.close();
  }
  
  private void validateHeader() throws IOException
  {
    long maxSCN = readMaxSCN();
    long copySCN = readCopySCN();
    
    // Array file is being updated if copySCN is greater than maxSCN. Should abort loading.
    if (maxSCN < copySCN) {
      throw new IOException("array file " + _file.getAbsolutePath() + " is being updated. Read maxSCN:" + maxSCN + " copySCN:" + copySCN);
    }
    
    log.info("read maxSCN:" + maxSCN);
    log.info("read copySCN:" + copySCN);
    
    int arrayLength = readArrayLength();
    assert arrayLength == _arrayLength;
    
    int elementSize = readElementSize();
    assert elementSize == _elementSize;
  }

  /**
   * Load the main array using the FastDataReadChannel for speed.
   * 
   * We don't allow loading the array file while an entry copy is happening
   * 
   * @return an int array
   * @throws IOException
   */
  public int[] loadIntArray() throws IOException
  {
    if (!_file.exists() || _file.length() == 0)
    {
      return null;
    }
    
    validateHeader();
    
    Chronos c = new Chronos();
    ChannelReader in = new ChannelReader(_file);
    
    try
    {
      in.open();
      in.position(DATA_START_POS);
      
      int[] array = new int[_arrayLength];
      for (int i = 0; i < _arrayLength; i++)
      {
        array[i] = in.readInt();
      }
      
      log.info("load array file " + _file.getAbsolutePath());
      log.info("data cache length:" + _arrayLength + " loaded in " + c.getElapsedTime());
      return array;
    }
    finally
    {
      in.close();
    }
  }
  
  /**
   * Load the main array using the FastDataReadChannel for speed.
   * 
   * We don't allow loading the array file while an entry copy is happening
   * 
   * @return a long array
   * @throws IOException
   */
  public long[] loadLongArray() throws IOException
  {
    if (!_file.exists() || _file.length() == 0)
    {
      return null;
    }
    
    validateHeader();
    
    Chronos c = new Chronos();
    ChannelReader in = new ChannelReader(_file);
    
    try
    {
      in.open();
      in.position(DATA_START_POS);
      
      long[] array = new long[_arrayLength];
      for (int i = 0; i < _arrayLength; i++)
      {
        array[i] = in.readLong();
      }
      
      log.info("load array file " + _file.getAbsolutePath());
      log.info("data cache length:" + _arrayLength + " loaded in " + c.getElapsedTime());
      return array;
    }
    finally
    {
      in.close();
    }
  }
  
  /**
   * Load the main array using the FastDataReadChannel for speed.
   * 
   * We don't allow loading the array file while an entry copy is happening
   * 
   * @return a short array
   * @throws IOException
   */
  public short[] loadShortArray() throws IOException
  {
    if (!_file.exists() || _file.length() == 0)
    {
      return null;
    }
    
    validateHeader();
    
    Chronos c = new Chronos();
    ChannelReader in = new ChannelReader(_file);
    
    try
    {
      in.open();
      in.position(DATA_START_POS);
        
      short[] array = new short[_arrayLength];
      for (int i = 0; i < _arrayLength; i++)
      {
        array[i] = in.readShort();
      }
      
      log.info("load array file " + _file.getAbsolutePath());
      log.info("data cache length:" + _arrayLength + " loaded in " + c.getElapsedTime());
      return array;
    }
    finally
    {
      in.close();
    }
  }
  
  private long getPosition(int index)
  {
    return DATA_START_POS + (index * _elementSize);
  }
  
  /**
   * Writes an int value at a specified position in the array file.
   * 
   * This method does not update copySCN and maxSCN in the array file.
   * 
   * @param pos a position in the array file.
   * @param val an int value
   * @throws IOException
   */
  public void writeInt(int pos, int val) throws IOException
  {
    _raf.seek(getPosition(pos));
    _raf.writeInt(val);
  }
  
  /**
   * Writes a long value at a specified position in the array file.
   * 
   * This method does not update copySCN and maxSCN in the array file.
   * 
   * @param pos a position in the array file.
   * @param val a long value
   * @throws IOException
   */
  public void writeLong(int pos, long val) throws IOException
  {
    _raf.seek(getPosition(pos));
    _raf.writeLong(val);
  }
  
  /**
   * Writes a short value at a specified position in the array file.
   * 
   * This method does not update copySCN and maxSCN in the array file.
   * 
   * @param pos a position in the array file.
   * @param val a short value
   * @throws IOException
   */
  public void writeShort(int pos, short val) throws IOException
  {
    _raf.seek(getPosition(pos));
    _raf.writeShort(val);
  }
  
  /**
   * Writes bytes at a specified position in the array file.
   * 
   * This method does not update copySCN and maxSCN in the array file.
   * 
   * @param pos a position in the array file.
   * @param val a array of bytes
   * @throws IOException
   */
  public void writeBytes(int pos, byte[] val) throws IOException
  {
    _raf.seek(getPosition(pos));
    _raf.write(val);
  }
  
  /**
   * Apply entries to the array file.
   * 
   * The method will flatten entry data and sort it by position.
   * So the array file can be updated sequentially to reduce disk seeking time.
   * 
   * This method updates copySCN and maxSCN in the array file.
   * 
   * @param entryList
   * @throws IOException
   */
  public synchronized <T extends EntryValue> void update(List<Entry<T>> entryList) throws IOException {
    Chronos chronos = new Chronos();
    
    // Sort values by position in the array file
    T[] values = EntryUtility.sortEntriesToValues(entryList);
    if (values == null) return;
    
    // Obtain maxSCN
    long maxScn = -1;
    for (Entry<?> e : entryList)
    {
      maxScn = Math.max(e.getMaxScn(), maxScn);
    }
    
    if(maxScn == -1)
    {
      log.info("update aborted: maxScn=" + maxScn);
      return;
    }
    
    // Write copySCN
    writeCopySCN(maxScn); 
    sync();
    log.info("update copySCN:" + maxScn);
    
    // Write entries data using file channel.
    ChannelWriter writer = new ChannelWriter(_file);
    writer.open();
    for (T v : values)
    {
      v.updateArrayFile(writer, getPosition(v.pos));
    }
    writer.close();
    
    // Write maxSCN
    writeMaxSCN(maxScn);
    sync(); 
    log.info("update maxSCN:" + maxScn);
    
    log.info(entryList.size() + " entries flushed to array file " + 
             _file.getAbsolutePath() + " in " + chronos.getElapsedTime());
  }
  
  public long readMaxSCN() throws IOException
  {
    _raf.seek(MAX_SCN_POS * 8);
    return _raf.readLong();
  }
  
  protected void writeMaxSCN(long value) throws IOException
  {
    _raf.seek(MAX_SCN_POS * 8);
    _raf.writeLong(value);
  }
  
  public long readCopySCN() throws IOException
  {
    _raf.seek(COPY_SCN_POS * 8);
    return _raf.readLong();
  }
  
  protected void writeCopySCN(long v) throws IOException
  {
    _raf.seek(COPY_SCN_POS * 8);
    _raf.writeLong(v);
  }
  
  public int readArrayLength() throws IOException
  {
    _raf.seek(ARRAY_LENGTH_POS * 8);
    return _raf.readInt();
  }
  
  protected void writeArrayLength(int v) throws IOException
  {
    _raf.seek(ARRAY_LENGTH_POS * 8);
    _raf.writeInt(v);
  }
  
  public int readElementSize() throws IOException
  {
    _raf.seek(ELEMENT_SIZE_POS * 8);
    return _raf.readInt();
  }
  
  protected void writeElementSize(int v) throws IOException
  {
    _raf.seek(ELEMENT_SIZE_POS * 8);
    _raf.writeInt(v);
  }
  
  public synchronized void reset(int[] intArray) throws IOException
  {
      FileChannel channel = _raf.getChannel();
      MappedByteBuffer mmapBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, _raf.length());
      
      mmapBuffer.position((int)DATA_START_POS);
      for(int i = 0; i < intArray.length; i++)
      {
          mmapBuffer.putInt(intArray[i]);
      }
      mmapBuffer.force();
      channel.close();
      
      // need reset
      _raf.close();
      _raf = new RandomAccessFile(_file, "rw");
  }
  
  public synchronized void reset(int[] intArray, long maxScn) throws IOException
  {   
      reset(intArray);
      
      // Write copySCN and maxSCN
      writeCopySCN(maxScn);
      writeMaxSCN(maxScn);
      sync();
      log.info("update copySCN and maxSCN:" + maxScn);
  }
  
  public synchronized void reset(long[] longArray) throws IOException
  {
      FileChannel channel = _raf.getChannel();
      MappedByteBuffer mmapBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, _raf.length());
      
      mmapBuffer.position((int)DATA_START_POS);
      for(int i = 0; i < longArray.length; i++)
      {
          mmapBuffer.putLong(longArray[i]);
      }
      mmapBuffer.force();
      channel.close();
      
      // need reset
      _raf.close();
      _raf = new RandomAccessFile(_file, "rw");
  }
  
  public synchronized void reset(long[] longArray, long maxScn) throws IOException
  {   
      reset(longArray);
      
      // Write copySCN and maxSCN
      writeCopySCN(maxScn);
      writeMaxSCN(maxScn);
      sync();
      log.info("update copySCN and maxSCN:" + maxScn);
  }
  
  public synchronized void reset(short[] shortArray) throws IOException
  {
      FileChannel channel = _raf.getChannel();
      MappedByteBuffer mmapBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, _raf.length());
      
      mmapBuffer.position((int)DATA_START_POS);
      for(int i = 0; i < shortArray.length; i++)
      {
          mmapBuffer.putShort(shortArray[i]);
      }
      mmapBuffer.force();
      channel.close();
      
      // need reset
      _raf.close();
      _raf = new RandomAccessFile(_file, "rw");
  }
  
  public synchronized void reset(short[] shortArray, long maxScn) throws IOException
  {   
      reset(shortArray);
      
      // Write copySCN and maxSCN
      writeCopySCN(maxScn);
      writeMaxSCN(maxScn);
      sync();
      log.info("update copySCN and maxSCN:" + maxScn);
  }
}
