import com.qf.executor.AnotherExecutors;

import java.util.Random;
import java.util.concurrent.ExecutorService;

/**
 * 版权：Copyright 2017 QuarkFinance IT
 * 描述：<描述>
 * 创建人：ShaozeWang
 * 创建时间：2017/8/16
 * 备注:
 */
public class AnotherExecutorsTest {
    public static void main(String[] args) {
        ExecutorService exec = AnotherExecutors.newFixedThreadPool(null,10);
//        exec.shutdownNow();
        exec= AnotherExecutors.newFixedThreadPool("xxx",10);
        while (true) {
            try {
                Thread.currentThread().sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.currentThread().sleep(new Random().nextInt(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread());
                }

                @Override
                public String toString() {
                    return "xx";
                }
            });
        }
    }
}
