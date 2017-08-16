
import com.qf.executor.PriorityTaskExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
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
        Assert.assertEquals(PriorityTaskExecutor.getOrBuild(name, 2).getName(),name);
        Assert.assertEquals(PriorityTaskExecutor.getOrBuild(name, 2),PriorityTaskExecutor.getOrBuild(name, 2));
        Assert.assertEquals(PriorityTaskExecutor.getOrBuild(name, 2),PriorityTaskExecutor.getOrBuild(name, 2));
        Assert.assertEquals(PriorityTaskExecutor.getOrBuild(name, 2),PriorityTaskExecutor.getOrBuild(name, 2));
    }

    @Test(expected=PriorityTaskExecutor.Task.WrongPriorityException.class)
    public void testWrongPriorityException(){
        new ReturnNameTask("name",1000);
    }

    @Test(expected =  PriorityTaskExecutor.NotRunningException.class)
    public void testNotRunningException(){
        PriorityTaskExecutor r = PriorityTaskExecutor.getOrBuild(name, 2);
        r.shutdown();
        r.addTask(new SleepTask(2));
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
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            // Ignore
        }
        String str = r.printQueue();
        Assert.assertTrue(str,str.contains("QueueSize:1"));
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
    public void testResize()  {
        int size=3;
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name,2);
        try {
            r.addTask(new SleepTask(3)).get();
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        Assert.assertEquals(r.getCount(),1);
        Assert.assertEquals(r.getThreadPoolSize(),2);
        r.resize(size);
        r.resize(size);
        Assert.assertEquals(r.getThreadPoolSize(),size);

    }

    private PriorityTaskExecutor.MyFutureTask sleepAndAddNewSleepTask(PriorityTaskExecutor r, int sleep){
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            //Ignore
        }
        return r.addTask(new SleepTask(sleep));
    }
    @Test
    public void testTaskPriority(){
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name);
        r.resize(1);
        for (int i = 0; i < 10; i++) {
            sleepAndAddNewSleepTask(r,100);
        }
        PriorityTaskExecutor.MyFutureTask<Object, SleepTask> fu2 = sleepAndAddNewSleepTask(r,100);
        PriorityTaskExecutor.MyFutureTask<Object, SleepTask> fu3 = sleepAndAddNewSleepTask(r,100);
        fu3.getRealTask().setPriority(PriorityTaskExecutor.Task.HIGH);
        try {
            TimeUnit.MILLISECONDS.sleep(200);//等待fu3执行完
            r.shutdown();
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        Exception ex=null;
        try {
            fu2.get();
        } catch (Exception e) {
            ex=e;
        }
        Assert.assertEquals(ex.getClass(),CancellationException.class);
        Assert.assertEquals(fu2.isCancelled(),true);

        try {
            Assert.assertNotNull(fu3.get());
        } catch (Exception e) {
            Assert.assertNull(e);
        }

        Assert.assertEquals(fu3.getRealTask().getPriority(), PriorityTaskExecutor.Task.HIGH);
        Assert.assertNotNull(fu3.getRealTask().getCreateTime());
    }

    @Test
    public void testShutdown2()  {

        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name);
        r.shutdown();
        Assert.assertEquals(r.getQueueSize(),0);
        Assert.assertTrue(r.isShutdown());
        r.shutdown();
        Assert.assertTrue(r.isShutdown());
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


    @Test
    public void testShutdownTaskCancel()  {
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name);
        r.setDebug(true);
        r.setStopExecutorTimeout(2000);//修改关闭超时为 100ms
        r.resize(1);
        PriorityTaskExecutor.MyFutureTask<Object,SleepTask> fu1 = r.addTask(new SleepTask("sleep", PriorityTaskExecutor.Task.NORMAL, 1000));
        PriorityTaskExecutor.MyFutureTask<Object,SleepTask> fu2 = r.addTask(new SleepTask("sleep", PriorityTaskExecutor.Task.NORMAL, 1000));
        r.shutdown();
        fu2.cancel(false);
        Exception ex=null;
        try {
            System.out.println(fu2.get());
        } catch (Exception e) {
            ex=e;
        }
        Assert.assertNotNull(ex);
        Assert.assertEquals(ex.getClass(),CancellationException.class);

    }


    @Test
    public void testRateSet()  {
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name);
        double rate=1000;
        Assert.assertFalse(r.isRateLimiterEnabled());
        r.resetRate(rate);
        Assert.assertEquals(rate,r.getRateLimit(),0.1);
    }

    @Test
    public void testRateLimit1()  {
        testRateLimit(1);
    }

    @Test
    public void testRateLimit2()  {
        testRateLimit(2);
    }

    @Test
    public void testRateLimit3()  {
        testRateLimit(3);
    }


    @Test
    public void testRunTimeException()  {

        PriorityTaskExecutor<Object,ExceptionTask> r = PriorityTaskExecutor.getOrBuild(name);
        PriorityTaskExecutor.MyFutureTask<Object, ExceptionTask> fu = r.addTask(new ExceptionTask());
        Exception ex=null;
        try {
            try {
                fu.get();
            } catch (InterruptedException e) {
                Assert.assertNull(e);
            }
        } catch (ExecutionException e) {
            ex=e;
        }
        Assert.assertNotNull(ex);
        Assert.assertEquals(ex.getCause().getClass(),RuntimeException.class);
        Assert.assertEquals(ex.getCause().getMessage(),ExceptionTask.class.getName());
    }


    @Test
    public void testRateReset()  {
        double rate=2;
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name+"rate");
        r.resetRate(rate);
        Assert.assertEquals(rate,r.getRateLimit(),0.1);
        double newRate=3;
        r.resetRate(newRate);
        Assert.assertEquals(newRate,r.getRateLimit(),0.1);
    }

    public void testRateLimit(int rate)  {
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name+"rate");
        r.resetRate(rate);
        r.resize(10);
        int taskCount=100;
        List< PriorityTaskExecutor.MyFutureTask<Object, SleepTask> > list=new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            list.add(r.addTask(new SleepTask(200)));
        }
        try {
            TimeUnit.MILLISECONDS.sleep(2);
            Assert.assertTrue(r.getQueueSize()>0);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testRateDisable()  {
        PriorityTaskExecutor<Object,SleepTask> r = PriorityTaskExecutor.getOrBuild(name);
        double rate=1000;
        r.resetRate(rate);
        Assert.assertEquals(rate,r.getRateLimit(),0.1);
        Assert.assertTrue(r.isRateLimiterEnabled());
        r.disableRateLimiter();
        Assert.assertFalse(r.isRateLimiterEnabled());
        Assert.assertEquals(r.getRateLimit(),0,0.1);
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

        public SleepTask(int sleep) {
            super("sleep", PriorityTaskExecutor.Task.NORMAL);
            this.sleep=sleep;
        }
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
            }this.threadName=Thread.currentThread().toString();
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



    static class ExceptionTask extends PriorityTaskExecutor.Task<Object> {

        public ExceptionTask() {
            super(ExceptionTask.class.getName(), PriorityTaskExecutor.Task.NORMAL);
        }

        @Override
        public Object call() {
            throw new RuntimeException(ExceptionTask.class.getName());
        }
    }
}
