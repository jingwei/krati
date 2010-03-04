package krati.zoie.impl;

public class ZoieMDSData {
	public final byte[] data;
	public final int id;
	public final boolean isDelete;
	public final int offset;
	public final int len;
	
	public ZoieMDSData(int id,byte[] data,int offset,int len,boolean isDelete){
		this.id=id;
		this.data=data;
		this.isDelete=isDelete;
		this.offset=offset;
		this.len=len;
	}
	
	public ZoieMDSData(int id,byte[] data,int offset,int len){
		this(id,data,offset,len,false);
	}
	
	public ZoieMDSData(int id,byte[] data){
		this(id,data,0,data.length,false);
	}
	
	public ZoieMDSData(int id){
		this(id,null,-1,-1,true);
	}
}
