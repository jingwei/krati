package test.zoie;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import krati.cds.DataCache;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;
import proj.zoie.api.ZoieIndexReader.SubZoieReaderAccessor;
import proj.zoie.impl.indexing.ZoieSystem;

@SuppressWarnings("unchecked")
public class ZoieDataCache<R extends IndexReader> implements DataCache {

	private final ZoieSystem<R,ZoieData> _zoieSystem;
	private final int _idStart;
	private final int _idCount;
	
	public ZoieDataCache(ZoieSystem<R,ZoieData> zoieSystem,int idStart,int idCount){
		_zoieSystem = zoieSystem;
		_idStart = idStart;
		_idCount = idCount;
	}
	
    @Override
	public void deleteData(int memberId, long scn) throws Exception {
		DataEvent<ZoieData> event = new DataEvent<ZoieData>(scn,new ZoieData(memberId));
		_zoieSystem.consume(Arrays.asList(event));
	}

	@Override
	public byte[] getData(int memberId) {
		return fetchData(memberId);
	}
	
	private final byte[] fetchData(int memberId) {
		List<ZoieIndexReader<R>> readerList = null;
		try{
			readerList = _zoieSystem.getIndexReaders();
			if (readerList.size()==0) return null;
			SubZoieReaderAccessor<R> accessor = ZoieIndexReader.getSubZoieReaderAccessor(readerList);
			SubReaderInfo<ZoieIndexReader<R>> info = accessor.geSubReaderInfoFromUID(memberId);
			if (info!=null){
			  Document doc = info.subreader.document(info.subdocid);
			  return doc.getBinaryValue(ZoieInterpreter.STORE_FIELD_NAME);
			}
			else{
			  return null;
			}
		}
		catch(IOException ioe)
		{
			ioe.printStackTrace();
			return new byte[0];
		}
		finally{
			if (readerList!=null){
				_zoieSystem.returnIndexReaders(readerList);
			}
		}
	}

	@Override
	public int getData(int memberId, byte[] dst) {
		byte[] data = fetchData(memberId);
		System.arraycopy(data, 0, dst, 0, data.length);
		return data.length;
	}

	@Override
	public int getData(int memberId, byte[] dst, int offset) {
		byte[] data = fetchData(memberId);
		System.arraycopy(data, 0, dst, offset, data.length);
		return data.length;
	}

	@Override
	public int getIdCount() {
		return _idCount;
	}

	@Override
	public int getIdStart() {
		return _idStart;
	}

	@Override
	public void persist() throws IOException {
		try {
			_zoieSystem.flushEvents(Long.MAX_VALUE);
		} catch (ZoieException e) {
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public void setData(int memberId, byte[] data, long scn) throws Exception {
		DataEvent<ZoieData> event = new DataEvent<ZoieData>(scn,new ZoieData(memberId,data));
		_zoieSystem.consume(Arrays.asList(event));
	}

	@Override
	public void setData(int memberId, byte[] data, int offset, int length, long scn) throws Exception {
		DataEvent<ZoieData> event = new DataEvent<ZoieData>(scn,new ZoieData(memberId,data,offset,length));
		_zoieSystem.consume(Arrays.asList(event));
	}

    @Override
    public long getHWMark() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getLWMark() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void saveHWMark(long endOfPeriod) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void clear() throws IOException {
        // TODO Auto-generated method stub
        
    }

}
