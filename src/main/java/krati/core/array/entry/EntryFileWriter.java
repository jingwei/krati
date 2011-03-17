package krati.core.array.entry;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.io.ChannelWriter;

/**
 * Transactional Redo Entry File Structure:
 * 
 * +------------------------+
 * |Head Section            |
 * |------------------------|
 * | STORAGE_VERSION | long | 
 * | MIN_SCN         | long |
 * | MAX_SCN         | long |
 * | ENTRY_VALUE_CNT | int  |
 * |------------------------|
 * |Data Section            |
 * |------------------------|
 * | POSITION   VALUE   SCN | (entry value 1)
 * | POSITION   VALUE   SCN | (entry value 2)
 * | ...                    | ...
 * |------------------------|
 * |Tail Section            |
 * |------------------------|
 * | MIN_SCN         | long |
 * | MAX_SCN         | long |
 * +------------------------+
 * 
 * @author jwu
 *
 * @param <T> Generic entry value representing an update to array file.
 */
public class EntryFileWriter {
    private final static Logger _log = Logger.getLogger(EntryFileWriter.class);
    
    private final static long STORAGE_VERESION_POSITION = 0;
    private final static long HEAD_MIN_SCN_POSITION     = 8;
    private final static long HEAD_MAX_SCN_POSITION     = 16;
    private final static long ENTRY_VALUE_CNT_POSITION  = 24;
    private final static long DATA_START_POSITION       = 28;
    
    private final ChannelWriter _writer;
    private int  _valCnt = 0;
    private long _minScn = 0;
    private long _maxScn = 0;
    
    public EntryFileWriter(File file) {
        this._writer = new ChannelWriter(file);
        this._valCnt = 0;
    }
    
    public File getFile() {
        return _writer.getFile();
    }
    
    public long getMinScn() {
        return _minScn;
    }
    
    public long getMaxScn() {
        return _maxScn;
    }
    
    public void open(long minScn, long maxScn) throws IOException {
        _writer.open();
        _valCnt = 0;
        _minScn = minScn;
        _maxScn = maxScn;
        
        // Update the tail section
        _writer.writeLong(STORAGE_VERESION_POSITION, Entry.STORAGE_VERSION);
        _writer.writeLong(HEAD_MIN_SCN_POSITION, minScn);
        _writer.writeLong(HEAD_MAX_SCN_POSITION, maxScn);
        _writer.writeInt(ENTRY_VALUE_CNT_POSITION, _valCnt);
        _writer.position(DATA_START_POSITION);
        
        _log.info("opened: minScn=" + _minScn + " maxScn="  + _maxScn + " valCnt=" + _valCnt + " file=" + _writer.getFile().getName());
    }
    
    public void close() throws IOException {
        // Update the total count of entry values
        _writer.writeInt(ENTRY_VALUE_CNT_POSITION, _valCnt);
        
        // Update the tail section
        _writer.writeLong(_minScn);
        _writer.writeLong(_maxScn);
        
        _writer.close();
        _log.info("closed: minScn=" + _minScn + " maxScn=" + _maxScn + " valCnt=" + _valCnt);
        
        _valCnt = 0;
        _minScn = 0;
        _maxScn = 0;
    }
    
    public void flush() throws IOException {
        _writer.flush();
    }
    
    public void write(int pos, int val, long scn) throws IOException {
        _writer.writeInt(pos);   /* array position */
        _writer.writeInt(val);   /* data value     */
        _writer.writeLong(scn);  /* SCN value      */
        _valCnt++;
    }
    
    public void write(int pos, long val, long scn) throws IOException {
        _writer.writeInt(pos);   /* array position */
        _writer.writeLong(val);  /* data value     */
        _writer.writeLong(scn);  /* SCN value      */
        _valCnt++;
    }
    
    public void write(int pos, short val, long scn) throws IOException {
        _writer.writeInt(pos);   /* array position */
        _writer.writeShort(val); /* data value     */
        _writer.writeLong(scn);  /* SCN value      */
        _valCnt++;
    }
}
