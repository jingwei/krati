package test.protos;

import krati.io.Serializer;
import test.protos.MemberProtos.Member;

import com.google.protobuf.InvalidProtocolBufferException;

public class MemberSerializer implements Serializer<MemberProtos.Member> {
    
    @Override
    public Member deserialize(byte[] binary) {
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