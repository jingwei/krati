package test.code;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import krati.core.array.AddressArray;
import krati.core.array.DataCorruptionException;
import krati.core.array.SimpleDataArray;
import krati.core.segment.Segment;
import krati.core.segment.SegmentManager;

/**
 * CheckedDataArray.
 * 
 * @author jwu
 *
 */
public class CheckedDataArray extends SimpleDataArray {
    private final static int CHECKSUM_LENGTH = 8;
    private byte[] _checkedData = new byte[4096];
    private ByteBuffer _checkedBuffer = ByteBuffer.wrap(_checkedData);
    
    public CheckedDataArray(AddressArray addressArray,
                            SegmentManager segmentManager) {
        this(addressArray, segmentManager, 0.5);
    }
    
    public CheckedDataArray(AddressArray addressArray,
                            SegmentManager segmentManager,
                            double segmentCompactFactor) {
        super(addressArray,
              segmentManager,
              segmentCompactFactor);
    }

    @Override
    public byte[] get(int index) {
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);

            // no data found
            if (segPos < Segment.dataStartPosition)
                return null;

            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);

            // read stored data length including checksum bytes
            int len = seg.readInt(segPos);

            if (len > CHECKSUM_LENGTH) {
                int dataLength = len - CHECKSUM_LENGTH;
                byte[] realData = new byte[dataLength];

                int realLength = getData(seg, segPos + 4, len, realData, 0);
                if (realLength == dataLength) {
                    return realData;
                }
            } else if (len == 0) {
                return new byte[0];
            }

            throw new DataCorruptionException(index);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public int get(int index, byte[] data, int offset) {
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);

            // no data found
            if (segPos < Segment.dataStartPosition)
                return -1;

            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);

            // read stored data length including checksum bytes
            int len = seg.readInt(segPos);

            // read data into byte array
            if (len > CHECKSUM_LENGTH) {
                int dataLength = len - CHECKSUM_LENGTH;
                int realLength = getData(seg, segPos + 4, len, data, offset);
                if (dataLength == realLength) {
                    return dataLength;
                }
            } else if (len == 0) {
                return 0;
            }

            throw new DataCorruptionException(index);
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public void set(int index, byte[] data, int offset, int length, long scn) throws Exception {
        if (length > 0) {
            // Calculate 8-byte checksum and combine it with real data
            int checkedLength = fillCheckedData(data, offset, length);
            super.set(index, _checkedData, 0, checkedLength, scn);
        } else {
            // No checksum for 0-sized data
            super.set(index, data, offset, length, scn);
        }
    }

    private long getChecksum(byte[] data, int offset, int length) {
        Checksum checksum = new Adler32();
        checksum.update(data, offset, length);
        return checksum.getValue();
    }

    private int getData(Segment seg, int readPos, int readLen, byte[] dst, int offset) throws IOException {
        long checksumOld, checksumNew;

        // read checksum
        checksumOld = seg.readLong(readPos);

        // read actual data
        int dataLen = readLen - CHECKSUM_LENGTH;
        seg.read(readPos + CHECKSUM_LENGTH, dst, offset, dataLen);

        // calculate data checksum
        Checksum checksum = new Adler32();
        checksum.update(dst, offset, dataLen);
        checksumNew = checksum.getValue();

        // compare checksum
        if (checksumNew != checksumOld) {
            return -1;
        }

        // return the true length of data without counting checksum bytes
        return dataLen;
    }

    private int fillCheckedData(byte[] data, int offset, int length) {
        ensureCheckedBuffer(length + CHECKSUM_LENGTH);
        long checksum = getChecksum(data, offset, length);

        _checkedBuffer.clear();
        _checkedBuffer.putLong(checksum);
        _checkedBuffer.put(data, offset, length);

        return _checkedBuffer.position();
    }

    private final void ensureCheckedBuffer(int length) {
        if (_checkedData.length < length) {
            _checkedData = new byte[length];
            _checkedBuffer = ByteBuffer.wrap(_checkedData);
        }
    }
}
