import com.qf.executor.PriorityTaskExecutor;

/**
 * 版权：Copyright 2017 QuarkFinance IT
 * 描述：<描述>
 * 创建人：ShaozeWang
 * 创建时间：2017/8/3
 * 备注:
 */
public class GcTest {
    public static void main(String[] args) {
        String name="test";

        System.gc();
        PriorityTaskExecutor a = PriorityTaskExecutor.getOrBuild(name);
        a.shutdown();
        a=null;
        System.gc();
        a = PriorityTaskExecutor.getOrBuild(name);
        a.shutdown();
//        a=null;
        System.gc();
    }
}
