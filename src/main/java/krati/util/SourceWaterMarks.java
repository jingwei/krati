package krati.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * SourceWaterMarks (not thread safe)
 * 
 * @author jwu
 * @version 0.4.2
 * @since 03/04, 2011
 * 
 * <p>
 * 08/18, 2011 - Added syncWaterMarks(String source) <br/>
 * 10/08, 2011 - Added getEntry(String source) <br/>
 */
public class SourceWaterMarks {
    private File file;
    private File fileOriginal;
    private final Map<String, WaterMarkEntry> sourceWaterMarkMap;
    private final static Logger logger = Logger.getLogger(SourceWaterMarks.class);
    
    /**
     * Create a new instance of SourceWaterMarks.
     * 
     * @param file - the file for storing water marks.
     */
    public SourceWaterMarks(File file) {
        this.file = file;
        this.fileOriginal = new File(file.getPath() + ".original");
        sourceWaterMarkMap = new HashMap<String, WaterMarkEntry>();

        // Load water marks for different sources
        try {
            loadWaterMarks();
            for (String source : sourceWaterMarkMap.keySet()) {
                WaterMarkEntry wmEntry = sourceWaterMarkMap.get(source);
                logger.info("Loaded water mark entry: " + wmEntry);
            }
        } catch (Exception ioe) {
            logger.error("Failed to load water marks");
        }
    }
    
    /**
     * Gets the file for storing water marks.
     */
    public File getFile() {
        return file;
    }
    
    /**
     * Gets the original file for storing water marks.
     */
    public File getFileOriginal() {
        return fileOriginal;
    }
    
    /**
     * Load water marks from underlying files.
     * 
     * @throws IOException
     */
    protected void loadWaterMarks() throws IOException {
        // Load source water marks from file
        if (file != null && file.exists()) {
            try {
                loadWaterMarks(file);
            } catch (IOException e) {
                if (fileOriginal != null && fileOriginal.exists()) {
                    loadWaterMarks(fileOriginal);
                }
            }
        }
    }
    
    /**
     * Load water marks from an underlying file.
     * 
     * @param waterMarksFile
     * @throws IOException
     */
    protected void loadWaterMarks(File waterMarksFile) throws IOException {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream(waterMarksFile);

        logger.info("Loading " + waterMarksFile);

        try {
            p.load(fis);

            Enumeration<?> enm = p.propertyNames();
            while (enm.hasMoreElements()) {
                String source = (String) enm.nextElement();
                String waterMarks = p.getProperty(source);
                String[] parts = waterMarks.split(",");
                if (parts.length == 2) {
                    long lwmScn = Long.parseLong(parts[0].trim());
                    long hwmScn = Long.parseLong(parts[1].trim());
                    if (hwmScn < lwmScn)
                        lwmScn = hwmScn;

                    WaterMarkEntry wmEntry = sourceWaterMarkMap.get(source);
                    if (wmEntry == null) {
                        wmEntry = new WaterMarkEntry(source);
                        sourceWaterMarkMap.put(source, wmEntry);
                    }

                    wmEntry.setLWMScn(lwmScn);
                    wmEntry.setHWMScn(hwmScn);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to load source water marks from " + waterMarksFile.getName(), e);
            throw e;
        } finally {
            fis.close();
            fis = null;
        }
    }
    
    /**
     * Gets the sources of this SourceWaterMarks.
     */
    public Set<String> sources() {
        return sourceWaterMarkMap.keySet();
    }
    
    /**
     * Gets the water mark entry of the specified <code>source</code>.
     * 
     * @param source - the source
     * @return the water mark entry mapped to the specified <code>source</code>.
     */
    public WaterMarkEntry getEntry(String source) {
        return sourceWaterMarkMap.get(source);
    }
    
    /**
     * Gets the high water mark of a source.
     * 
     * @param source - the source
     */
    public long getHWMScn(String source) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        return (e == null) ? 0 : e.getHWMScn();
    }
    
    /**
     * Sets the high water mark of a source.
     * This method has the same functionality as {{@link #saveHWMark(String, long)}.
     * 
     * @param source - the source
     * @param scn    - the water mark value
     */
    public void setHWMScn(String source, long scn) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        if (e == null) {
            e = new WaterMarkEntry(source);
            sourceWaterMarkMap.put(source, e);
        }
        e.setHWMScn(scn);
    }
    
    /**
     * Gets the low water mark of a source.
     * 
     * @param source - the source
     */
    public long getLWMScn(String source) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        return (e == null) ? 0 : e.getLWMScn();
    }
    
    /**
     * Sets the low water mark of a source
     * 
     * @param source - the source
     * @param scn    - the water mark value
     */
    public void setLWMScn(String source, long scn) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        if (e == null) {
            e = new WaterMarkEntry(source);
            sourceWaterMarkMap.put(source, e);
        }
        e.setLWMScn(scn);
    }
    
    /**
     * Saves the high water mark of a source.
     * This method has the same functionality as {@link #setHWMScn(String, long)}.
     * 
     * @param source - the source
     * @param hwm    - the high water mark
     */
    public void saveHWMark(String source, long hwm) {
        WaterMarkEntry wmEntry = sourceWaterMarkMap.get(source);
        if (wmEntry != null) {
            wmEntry.setHWMScn(Math.max(hwm, wmEntry.getHWMScn()));
        } else {
            wmEntry = new WaterMarkEntry(source);
            wmEntry.setHWMScn(hwm);

            sourceWaterMarkMap.put(source, wmEntry);
        }
    }
    
    /**
     * Sets the water marks of a source.
     * 
     * @param source - the source
     * @param lwmScn - the low water mark SCN
     * @param hwmScn - the high water mark SCN
     */
    public void setWaterMarks(String source, long lwmScn, long hwmScn) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        if (e == null) {
            e = new WaterMarkEntry(source);
            sourceWaterMarkMap.put(source, e);
        }
        e.setLWMScn(lwmScn);
        e.setHWMScn(hwmScn);
    }
    
    /**
     * Sets and flushes the water marks of a source.
     * 
     * @param source - the source
     * @param lwmScn - the low water mark SCN
     * @param hwmScn - the high water mark SCN
     * @return <tt>true</tt> if flush is successful.
     */
    public boolean syncWaterMarks(String source, long lwmScn, long hwmScn) {
        setWaterMarks(source, lwmScn, hwmScn);
        return flush();
    }
    
    /**
     * Sync up the low water mark to the high water mark for a source.
     * 
     * @param source - the source
     * @return <tt>true</tt> if flush is successful.
     */
    public boolean syncWaterMarks(String source) {
        setLWMScn(source, getHWMScn(source));
        return flush();
    }
    
    /**
     * Sync up low water marks to high water marks for all the sources.
     * 
     * @return <tt>true</tt> if flush is successful.
     */
    public boolean syncWaterMarks() {
        // Sync up low water marks
        for (String source : sourceWaterMarkMap.keySet()) {
            WaterMarkEntry wmEntry = sourceWaterMarkMap.get(source);
            wmEntry.setLWMScn(wmEntry.getHWMScn());
        }
        
        return flush();
    }
    
    /**
     * Flushes low water marks and high water marks for all the sources.
     * 
     * @return <tt>true</tt> if flush is successful.
     */
    public boolean flush() {
        boolean ret = true;
        PrintWriter out = null;
        
        // Save source water marks
        try {
            // Backup the original file
            if (file.exists()) {
                if (fileOriginal.exists()) {
                    fileOriginal.delete();
                }

                file.renameTo(fileOriginal);
            }

            // Overwrite the existing file
            out = new PrintWriter(new FileOutputStream(file));
            for (String source : sourceWaterMarkMap.keySet()) {
                WaterMarkEntry wmEntry = sourceWaterMarkMap.get(source);
                out.println(wmEntry);
            }
            out.flush();
        } catch (IOException ioe) {
            logger.error("Failed to flush water marks", ioe);
            ret = false;
        } finally {
            if (out != null) {
                out.close();
                out = null;
            }
        }

        return ret;
    }
    
    /**
     * Clears SourceWaterMark.
     */
    public void clear() {
        sourceWaterMarkMap.clear();
        flush();
        if (fileOriginal != null && fileOriginal.exists()) {
            fileOriginal.delete();
        }
    }

    public static class WaterMarkEntry {
        private volatile long lwmScn = 0;
        private volatile long hwmScn = 0;
        private final String source;

        public WaterMarkEntry(String source) {
            this.source = source;
        }

        public String getSource() {
            return source;
        }

        public long getLWMScn() {
            return lwmScn;
        }

        public void setLWMScn(long scn) {
            this.lwmScn = scn;
        }

        public long getHWMScn() {
            return hwmScn;
        }

        public void setHWMScn(long scn) {
            this.hwmScn = scn;
        }

        public String toString() {
            return source + "=" + lwmScn + "," + hwmScn;
        }

        @Override
        public int hashCode() {
            int hash = (int) ((lwmScn & hwmScn) >> 32);
            hash += source.hashCode();
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;

            if (o == null)
                return false;

            if (o instanceof WaterMarkEntry) {
                WaterMarkEntry e = (WaterMarkEntry) o;
                return source.equals(e.source) && lwmScn == e.lwmScn && hwmScn == e.hwmScn;
            }

            return false;
        }
    }
}
