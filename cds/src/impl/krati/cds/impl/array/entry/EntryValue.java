package krati.cds.impl.array.entry;

import java.io.IOException;
import java.io.RandomAccessFile;

import krati.io.ChannelWriter;
import krati.io.DataWriter;

/**
 * EntryValue.
 * 
 * @author jwu
 */
public abstract class EntryValue implements Comparable<EntryValue> {
  public final int pos;   // position in the array
  public final long scn;  // SCN associated with an update at the position
  
  public EntryValue(int pos, long scn)
  {
    this.pos = pos;
    this.scn = scn;
  }
  
  @Override
  public String toString()
  {
    return pos + ":?:" + scn;
  }
  
  public int compareTo(EntryValue o)
  {
    return (pos < o.pos ? -1 : (pos == o.pos ? (scn < o.scn ? -1 : (scn == o.scn ? 0 : 1)) : 1));
  }
  
  /**
   * Writes this EntryValue to entry log file via a channel writer.
   * 
   * @param writer
   * @throws IOException
   */
  public abstract void write(DataWriter writer) throws IOException;
  
  /**
   * Writes this EntryValue to an random access file at a given position.
   * 
   * @param raf
   * @param position
   * @throws IOException
   */
  public abstract void updateArrayFile(RandomAccessFile raf, long position) throws IOException;
  
  /**
   * Writes this EntryValue to a file channel at a given position.
   * 
   * @param writer
   * @param position
   * @throws IOException
   */
  public abstract void updateArrayFile(ChannelWriter writer, long position) throws IOException;
}
