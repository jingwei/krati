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
 * 03/04, 2011
 * 
 */
public class SourceWaterMarks {
    private File file;
    private File fileOriginal;
    private final Map<String, WaterMarkEntry> sourceWaterMarkMap;
    private final static Logger logger = Logger.getLogger(SourceWaterMarks.class);

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

    public File getFile() {
        return file;
    }

    public File getFileOriginal() {
        return fileOriginal;
    }

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

    public Set<String> sources() {
        return sourceWaterMarkMap.keySet();
    }

    public long getHWMScn(String source) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        return (e == null) ? 0 : e.getHWMScn();
    }

    public void setHWMScn(String source, long scn) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        if (e == null) {
            e = new WaterMarkEntry(source);
            sourceWaterMarkMap.put(source, e);
        }
        e.setHWMScn(scn);
    }

    public long getLWMScn(String source) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        return (e == null) ? 0 : e.getLWMScn();
    }

    public void setLWMScn(String source, long scn) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        if (e == null) {
            e = new WaterMarkEntry(source);
            sourceWaterMarkMap.put(source, e);
        }
        e.setLWMScn(scn);
    }

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

    public void setWaterMarks(String source, long lwmScn, long hwmScn) {
        WaterMarkEntry e = sourceWaterMarkMap.get(source);
        if (e == null) {
            e = new WaterMarkEntry(source);
            sourceWaterMarkMap.put(source, e);
        }
        e.setLWMScn(lwmScn);
        e.setHWMScn(hwmScn);
    }

    public boolean syncWaterMarks(String source, long lwmScn, long hwmScn) {
        setWaterMarks(source, lwmScn, hwmScn);
        return syncWaterMarks();
    }

    public boolean syncWaterMarks() {
        boolean ret = true;
        PrintWriter out = null;

        // Sync up low water marks
        for (String source : sourceWaterMarkMap.keySet()) {
            WaterMarkEntry wmEntry = sourceWaterMarkMap.get(source);
            wmEntry.setLWMScn(wmEntry.getHWMScn());
        }

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
            logger.error("Failed to sync water marks", ioe);
            ret = false;
        } finally {
            if (out != null) {
                out.close();
                out = null;
            }
        }

        return ret;
    }

    public void clear() {
        sourceWaterMarkMap.clear();
        syncWaterMarks();
        if (fileOriginal != null && fileOriginal.exists()) {
            fileOriginal.delete();
        }
    }

    static class WaterMarkEntry {
        private long lwmScn = 0;
        private long hwmScn = 0;
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
