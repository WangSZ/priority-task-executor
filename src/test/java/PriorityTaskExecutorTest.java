
import com.qf.executor.PriorityTaskExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 版权：Copyright 2017 QuarkFinance IT
 * 描述：<描述>
 * 创建人：ShaozeWang
 * 创建时间：2017/8/2
 * 备注:
 */
public class PriorityTaskExecutorTest {

    final static String name = "HK";
    @Before
    public void before(){
        PriorityTaskExecutor.getOrBuild(name, 2).setDebug(true);
    }

    @After
    public void after(){
        PriorityTaskExecutor.shutdownAll();
    }

    /*
    PriorityTaskExecutor无论获取多少次，只要未关闭，应该放回同一个对象
     */
    @Test
    public void testInstanceEqual(){

        Assert.assertEquals(PriorityTaskExecutor.getOrBuild(name, 2),PriorityTaskExecutor.getOrBuild(name, 2));
        Assert.assertEquals(PriorityTaskExecutor.getOrBuild(name, 2),PriorityTaskExecutor.getOrBuild(name, 2));
        Assert.assertEquals(PriorityTaskExecutor.getOrBuild(name, 2),PriorityTaskExecutor.getOrBuild(name, 2));
    }


    /*
    PriorityTaskExecutor关闭后重新获取，应该生成新的对象
     */
    @Test
    public void testInstanceRecreate(){
        PriorityTaskExecutor first = PriorityTaskExecutor.getOrBuild(name, 2);
        first.setDebug(true);
        PriorityTaskExecutor second = PriorityTaskExecutor.getOrBuild(name, 2);
        Assert.assertEquals(first,second);
        second.shutdown();
        second = PriorityTaskExecutor.getOrBuild(name, 2);
        second.setDebug(true);
        Assert.assertNotEquals(first,second);
    }

    @Test
    public void testRunOnce() throws ExecutionException, InterruptedException {
        String taskName="abc";
        PriorityTaskExecutor.MyFutureTask fu = PriorityTaskExecutor.getOrBuild(name, 2).addTask(new ReturnNameTask(taskName, PriorityTaskExecutor.Task.NORMAL));
        Assert.assertEquals(taskName,fu.get());
    }

    @Test
    public void testTaskCancel()  {
        String taskName="abc";
        Exception ex=null;
        SleepTask s10=new SleepTask(taskName, PriorityTaskExecutor.Task.NORMAL,10);
        SleepTask s20=new SleepTask(taskName, PriorityTaskExecutor.Task.NORMAL,20);
        PriorityTaskExecutor r = PriorityTaskExecutor.getOrBuild(name);
        r.resize(1);//队列大小调整为1，方便后面模拟队列积压，并取消任务

        PriorityTaskExecutor.MyFutureTask fu20 = r.addTask(s20);
        PriorityTaskExecutor.MyFutureTask fu10 = r.addTask(s10);

        r.cancelTask(fu10,false);

        try {
            Assert.assertNotNull(fu20.get());
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        try {
            System.out.println(fu10.get());
        } catch (Exception e) {
           ex=e;
        }
        Assert.assertEquals(CancellationException.class,ex.getClass());

    }

    @Test
    public void testPrintQueue()  {

        PriorityTaskExecutor r = PriorityTaskExecutor.getOrBuild(name);
        r.resize(1);
        PriorityTaskExecutor.MyFutureTask fu1 = r.addTask(new SleepTask("test", PriorityTaskExecutor.Task.NORMAL, 10000));
        PriorityTaskExecutor.MyFutureTask fu2 = r.addTask(new SleepTask("test", PriorityTaskExecutor.Task.NORMAL, 10000));
        Assert.assertTrue(r.printQueue().contains("QueueSize:1"));
        r.cancelTask(fu1,false);
        r.cancelTask(fu2,false);
        Assert.assertTrue(r.printQueue().contains("QueueSize:0"));

    }

    @Test
    public void testCounterTask()  {
        PriorityTaskExecutor r = PriorityTaskExecutor.getOrBuild(name);
        r.setDebug(false);
        r.resize(5);
        AtomicLong c=new AtomicLong();
        int count=100;
        List<PriorityTaskExecutor.MyFutureTask> l=new ArrayList();
        for (int i = 0; i < count; i++) {
            l.add( r.addTask(new CounterTask(c)));
        }

        for (int i = 0; i < l.size(); i++) {
            try {
                l.get(i).get();
            } catch (Exception e) {
                Assert.assertNull(e);
            }
        }
        Assert.assertEquals(count,c.longValue());

    }



    @Test
    public void testShutdownTimeout()  {
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name);
        r.setDebug(true);
        r.setStopExecutorTimeout(100);//修改关闭超时为 100ms
        r.resize(1);
        PriorityTaskExecutor.MyFutureTask<Object,SleepTask> fu1 = r.addTask(new SleepTask("sleep", PriorityTaskExecutor.Task.NORMAL, 1000));
        r.resize(2);
        PriorityTaskExecutor.MyFutureTask<Object,SleepTask> fu2 = r.addTask(new SleepTask("sleep", PriorityTaskExecutor.Task.NORMAL, 10));
        try {
            Assert.assertEquals(fu1.get(),"sleep1000");
            Assert.assertEquals(fu2.get(),"sleep10");
            //因为提交fu1后，重新设置了executor， 故fu1和fu2应该在不同的线程执行
            Assert.assertNotEquals(fu1.getRealTask().getThreadName(),fu2.getRealTask().getThreadName());
        } catch (Exception e) {
            Assert.assertNull(e);
        }

    }

    static class ReturnNameTask extends PriorityTaskExecutor.Task<String>{

        public ReturnNameTask(String name, int priority) {
            super(name, priority);
        }

        @Override
        public String call() {
            return getName();
        }
    }

    static class SleepTask extends PriorityTaskExecutor.Task<Object> {

        public SleepTask(String name, int priority,int sleep) {
            super(name, priority);
            this.sleep=sleep;
        }
        private int sleep;

        public String getThreadName() {
            return threadName;
        }

        private String threadName;

        @Override
        public Object call() {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("SleepTask " + getName() + " "+sleep+ " DONE");
            this.threadName=Thread.currentThread().toString();
            return getName()+sleep;
        }
    }



    static class CounterTask extends PriorityTaskExecutor.Task<Object> {

        private AtomicLong counter;
        public CounterTask(AtomicLong counter) {
            super("demo", PriorityTaskExecutor.Task.NORMAL);
            this.counter=counter;
        }

        @Override
        public Object call() {
            return counter.incrementAndGet();
        }
    }
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        test();
    }

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
