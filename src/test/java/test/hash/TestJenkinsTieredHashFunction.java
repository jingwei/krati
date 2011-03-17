package test.hash;

import krati.util.HashFunction;
import krati.util.JenkinsHashFunction;

public class TestJenkinsTieredHashFunction extends EvalTieredHashFunction {
    public TestJenkinsTieredHashFunction() {
        super(TestJenkinsTieredHashFunction.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new JenkinsHashFunction();
    }
}
