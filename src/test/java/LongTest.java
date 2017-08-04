import com.qf.executor.PriorityTaskExecutor;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 版权：Copyright 2017 QuarkFinance IT
 * 描述：<描述>
 * 创建人：ShaozeWang
 * 创建时间：2017/8/4
 * 备注:
 */
public class LongTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        PriorityTaskExecutor r = PriorityTaskExecutor.getOrBuild(name, 2);
        r.resize(10);
        r.resetRate(10);
        test();
    }

    static String name="LongTest";

    public static class HkTask extends PriorityTaskExecutor.Task<Object> {

        public HkTask(String name, int priority) {
            super(name, priority);
        }

        @Override
        public Object call() {
            try {
                System.out.println("HkTask " + getName() + " DONE");
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return new Date().toString();
        }
    }

    public static void test()throws ExecutionException, InterruptedException{

        new Thread() {
            @Override
            public void run() {
                int j = 0;
                while (true) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    PriorityTaskExecutor r = PriorityTaskExecutor.getOrBuild(name, 2);
                    r.addTask(new HkTask("p-" + j++, PriorityTaskExecutor.Task.NORMAL));
                }
            }
        }.start();
        new Thread() {
            @Override
            public void run() {
                int j = 0;
                while (true) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    PriorityTaskExecutor r = PriorityTaskExecutor.getOrBuild(name, 2);
                    r.addTask(new HkTask("pt-" + j++, PriorityTaskExecutor.Task.HIGH));
                }
            }
        }.start();
        TimeUnit.DAYS.sleep(2);
    }
}
