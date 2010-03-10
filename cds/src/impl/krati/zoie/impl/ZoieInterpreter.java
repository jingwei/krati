package krati.zoie.impl;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

public class ZoieInterpreter extends AbstractZoieIndexableInterpreter<ZoieData> {
	public static final String STORE_FIELD_NAME = "_ZOIE_STORE_";
	@Override
	
	public ZoieIndexable convertAndInterpret(final ZoieData src) {
		return new ZoieIndexable(){

			@Override
			public IndexingReq[] buildIndexingReqs() {
				if (src.data!=null){
					Document doc = new Document();
					Field f = new Field(STORE_FIELD_NAME,src.data,Store.YES);
					doc.add(f);
					return new IndexingReq[]{new IndexingReq(doc)};
				}
				else{
					return null;
				}
			}

			@Override
			public long getUID() {
				return src.id;
			}

			@Override
			public boolean isDeleted() {
				return src.isDelete;
			}

			@Override
			public boolean isSkip() {
				return false;
			}
			
		};
	}

}
