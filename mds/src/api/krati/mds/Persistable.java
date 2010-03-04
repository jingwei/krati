package krati.mds;

import java.io.IOException;

/**
 * Persistable
 * 
 * @author jwu
 *
 */
public interface Persistable
{
  public void persist() throws IOException;
  
  public long getLWMark();
  
  public long getHWMark();
  
  public void saveHWMark(long endOfPeriod) throws Exception;
}
