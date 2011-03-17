package test.hash;

import krati.util.JenkinsHashFunction;
import krati.util.HashFunction;

public class TestJenkinsHashFunction extends EvalHashFunction {
    public TestJenkinsHashFunction() {
        super(TestJenkinsHashFunction.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new JenkinsHashFunction();
    }
}
