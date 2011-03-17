package krati.core.array.entry;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.io.ChannelReader;
import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.FastDataWriter;
import krati.util.Chronos;

/**
 * Entry.
 * 
 * Transactional Redo Entry.
 * 
 * @author jwu
 * 
 */
public abstract class AbstractEntry<T extends EntryValue> implements Entry<T> {
    private static final Logger _log = Logger.getLogger(Entry.class);

    protected long _minScn = 0;
    protected long _maxScn = 0;
    protected final int _entryId;
    protected final EntryValueFactory<T> _valFactory;
    protected int _entryCapacity;

    private int _entryServiceId = 0;
    private File _entryFile = null;

    /**
     * Create a new entry to hold updates to an array.
     * 
     * @param entryId
     *            The Id of this Entry.
     * @param valFactory
     *            The factory for manufacturing EntryValue(s).
     * @param initialCapacity
     *            The initial number of values this entry can hold.
     */
    protected AbstractEntry(int entryId, EntryValueFactory<T> valFactory, int initialCapacity) {
        this._entryId = entryId;
        this._valFactory = valFactory;
        this._entryCapacity = initialCapacity;
    }

    @Override
    public final int getId() {
        return _entryId;
    }

    @Override
    public final int getServiceId() {
        return _entryServiceId;
    }

    @Override
    public final void setServiceId(int serviceId) {
        _entryServiceId = serviceId;
    }

    /**
     * Gets the Entry file.
     * 
     * @return this Entry's file.
     */
    @Override
    public File getFile() {
        return _entryFile;
    }

    /**
     * Get the minimum SCN of updates maintained by this Entry.
     */
    @Override
    public final long getMinScn() {
        return _minScn;
    }

    /**
     * Get the maximum SCN of updates maintained by this Entry.
     */
    @Override
    public final long getMaxScn() {
        return _maxScn;
    }

    @Override
    public final EntryValueFactory<T> getValueFactory() {
        return _valFactory;
    }

    @Override
    public final int capacity() {
        return _entryCapacity;
    }

    @Override
    public final boolean isFull() {
        return size() >= _entryCapacity;
    }

    @Override
    public final boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void clear() {
        _minScn = 0;
        _maxScn = 0;
    }

    @Override
    public int compareTo(Entry<T> e) {
        return (_maxScn < e.getMaxScn() ? -1 : (_maxScn == e.getMaxScn() ? (_minScn < e.getMinScn() ? -1 : (_minScn == e.getMinScn() ? 0 : 1)) : 1));
    }

    /**
     * Saves to a file.
     * 
     * @param file
     * @throws IOException
     */
    public final void save(File file) throws IOException {
        _entryFile = file;
        Chronos c = new Chronos();
        DataWriter out = new FastDataWriter(file);

        try {
            out.open();

            // Save entry head
            out.writeLong(STORAGE_VERSION); // write the entry file version
            out.writeLong(_minScn);
            out.writeLong(_maxScn);
            out.writeInt(size());

            // Save entry body
            saveDataSection(out);

            // Save entry tail.
            out.writeLong(_minScn);
            out.writeLong(_maxScn);
        } finally {
            out.close();
        }

        _log.info("Saved entry: minScn=" + _minScn + " maxScn=" + _maxScn + " size=" + size() + " file=" + file.getName() + " in " + c.getElapsedTime());
    }

    /**
     * Loads an entry from a given file.
     * 
     * @param file
     * @throws IOException
     */
    public void load(File file) throws IOException {
        _entryFile = file;
        Chronos c = new Chronos();
        ChannelReader in = new ChannelReader(file);

        try {
            in.open();

            // Read entry head
            long fileVersion = in.readLong();
            if (fileVersion != STORAGE_VERSION) {
                throw new RuntimeException("Wrong storage version " + fileVersion + " encounted in " + file.getAbsolutePath() + ". Version " + STORAGE_VERSION
                        + " expected.");
            }

            long minScnHead = in.readLong();
            long maxScnHead = in.readLong();
            int length = in.readInt();

            // Read entry body
            loadDataSection(in, length);

            // Read entry tail
            long minScnTail = in.readLong();
            long maxScnTail = in.readLong();

            if (minScnHead != minScnTail) {
                throw new IOException("min scns don't match: " + minScnHead + " vs " + minScnTail);
            }
            if (maxScnHead != maxScnTail) {
                throw new IOException("max scns don't match:" + maxScnHead + " vs " + maxScnTail);
            }

            _minScn = minScnHead;
            _maxScn = maxScnHead;

            _log.info("loaded entry: minScn=" + _minScn + " maxScn=" + _maxScn + " size=" + size() + " file=" + file.getName() + " in " + c.getElapsedTime());
        } finally {
            in.close();
        }
    }

    abstract protected void saveDataSection(DataWriter out) throws IOException;

    abstract protected void loadDataSection(DataReader in, int cnt) throws IOException;

    protected final void maintainScn(long scn) {
        _maxScn = Math.max(_maxScn, scn);
        _minScn = (_minScn == 0) ? scn : Math.min(_minScn, scn);
    }

}
