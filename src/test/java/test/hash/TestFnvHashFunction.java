package test.hash;

import krati.util.FnvHashFunction;
import krati.util.HashFunction;

public class TestFnvHashFunction extends EvalHashFunction {
    public TestFnvHashFunction() {
        super(TestFnvHashFunction.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new FnvHashFunction();
    }
}
