package test.protos;

import krati.sos.ObjectSerializer;
import test.protos.MemberProtos.Member;

import com.google.protobuf.InvalidProtocolBufferException;

public class MemberSerializer implements ObjectSerializer<MemberProtos.Member> {
    
    @Override
    public Member construct(byte[] binary) {
        try {
            return Member.parseFrom(binary);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }
    
    @Override
    public byte[] serialize(Member object) {
        return object.toByteArray();
    }
    
}