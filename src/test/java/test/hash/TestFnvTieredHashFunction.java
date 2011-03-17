package test.hash;

import krati.util.HashFunction;
import krati.util.FnvHashFunction;

public class TestFnvTieredHashFunction extends EvalTieredHashFunction {
    public TestFnvTieredHashFunction() {
        super(TestFnvTieredHashFunction.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new FnvHashFunction();
    }
}
