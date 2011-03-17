package test.core;

import java.io.File;
import java.util.Random;

/**
 * MemberDataUpdate
 * 
 * @author jwu
 * 
 */
public class MemberDataUpdate {
    static long scn = 0;
    static Random random = new Random(System.currentTimeMillis());
    
    public final int _memberId;
    public final int _data;
    public final long _scn;
    
    private MemberDataUpdate(int memberId, int data, long scn) {
        this._memberId = memberId;
        this._data = data;
        this._scn = scn;
    }
    
    public int getMemberId() {
        return _memberId;
    }
    
    public int getData() {
        return _data;
    }
    
    public long getScn() {
        return _scn;
    }
    
    public static MemberDataUpdate[] generateUpdates(int memberIdStart, int memberIdCount) {
        MemberDataUpdate[] updates = new MemberDataUpdate[memberIdCount];
        
        for (int i = 0; i < memberIdCount; i++) {
            int memberId = memberIdStart + i;
            int data = random.nextInt(1000000);
            updates[i] = new MemberDataUpdate(memberId, data, scn++);
        }
        
        return updates;
    }
    
    public static void outputMemberData(File outputFile, MemberDataUpdate[] updates) {
        try {
            java.io.PrintWriter out = new java.io.PrintWriter(new java.io.FileOutputStream(outputFile));

            for (MemberDataUpdate u : updates) {
                out.printf("%10d=%10d%n", u.getMemberId(), u.getData());
            }
            out.flush();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
