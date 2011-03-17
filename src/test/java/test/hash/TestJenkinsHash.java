package test.hash;

import krati.util.HashFunction;
import krati.util.JenkinsHashFunction;

public class TestJenkinsHash extends EvalHash {
    public TestJenkinsHash() {
        super(TestJenkinsHash.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new JenkinsHashFunction();
    }
}
