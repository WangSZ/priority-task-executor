package com.qf.executor;

import com.google.common.util.concurrent.RateLimiter;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 版权：Copyright 2017 QuarkFinance IT
 * 描述：<描述>
 * 创建人：ShaozeWang
 * 创建时间：2017/8/1
 * 备注:
 */
public class PriorityTaskExecutor<V,T  extends PriorityTaskExecutor.Task<V>> implements PriorityTaskExecutorMBean {


    private PriorityTaskExecutor() {
    }


    private final static ConcurrentHashMap<String, PriorityTaskExecutor> cache = new ConcurrentHashMap<>();

    public static PriorityTaskExecutor getOrBuild(String name, int size) {
        PriorityTaskExecutor r = cache.get(name);
        if (r == null) {
            synchronized (cache) {;
                if (!cache.containsKey(name)) {
                    cache.put(name, build(name, size));
                }
            }
            r = cache.get(name);
        }
        return r;
    }

    private static PriorityTaskExecutor build(String name, int size) {
        PriorityTaskExecutor r = new PriorityTaskExecutor();
        r.name = name;
        r.size = size;
        r.init();
        r.registerMBean();
        return r;
    }


    private void unregisterMBean() {
        try {
            ObjectName objectName = new ObjectName("com.qf.jmx.PriorityTaskExecutor:type=" + (this.name == null ? this.toString() : this.name));
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(objectName);
        } catch (JMException e) {
            // ignore
        }
    }

    private void registerMBean() {
        try {
            ObjectName objectName = new ObjectName("com.qf.jmx.PriorityTaskExecutor:type=" + (this.name == null ? this.toString() : this.name));
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, objectName);
        } catch (JMException e) {
            // ignore
        }
    }


    public static PriorityTaskExecutor getOrBuild(String name) {
        return getOrBuild(name, 1);
    }

    public String getName() {
        return name;
    }

    private String name;

    private int size = 1;
    private AtomicLong count = new AtomicLong(0);
    private PriorityBlockingQueue<MyFutureTask<V,T>> queue = new PriorityBlockingQueue<>();


    private ExecutorService executor;
    private ReentrantLock mainLock = new ReentrantLock();
    private ReentrantReadWriteLock rateLimiterLock = new ReentrantReadWriteLock();
    private volatile int status = 0;

    public boolean isRateLimiterEnabled() {
        return rateLimiterEnabled&&rateLimiter!=null;
    }

    private volatile boolean rateLimiterEnabled = false;
    private RateLimiter rateLimiter;

    public static final int SHUTDOWN = 0;
    public static final int RUNNING = 1;
    public static final int RESIZING = 2;


    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public final double getRateLimit(){
        if(!isRateLimiterEnabled()){
            return 0;
        }
        return rateLimiter.getRate();
    }

    @Override
    public final void resetRate(double permitsPerSecond) {
        rateLimiterLock.writeLock().lock();
        try{
            if(rateLimiter==null){
                rateLimiter=RateLimiter.create(permitsPerSecond);
            }else{
                rateLimiter.setRate(permitsPerSecond);
            }
            rateLimiterEnabled=true;
        }finally {
            rateLimiterLock.writeLock().unlock();
        }
    }

    public void disableRateLimiter(){
        rateLimiterLock.writeLock().lock();
        try{
            rateLimiterEnabled=false;
            rateLimiter=null;
        }finally {
            rateLimiterLock.writeLock().unlock();
        }
    }



    public boolean isDebug() {
        return debug;
    }

    private boolean debug;

    public static void shutdownAll(){
        Enumeration<String> keys = cache.keys();
        List<String> keyList=new ArrayList<>();
        while (keys.hasMoreElements()){
            keyList.add(keys.nextElement());
        }
        for (int i = 0; i <keyList.size(); i++) {
            PriorityTaskExecutor pte = cache.remove(keyList.get(i));
            pte.shutdown();
        }
    }


    public void shutdown() {
        mainLock.lock();
        try {
            if (status == SHUTDOWN) {
                if (isDebug()) {
                    System.out.println(this+" already shutdown");
                }
                return;
            }
            status = SHUTDOWN;
            stopExecutor();
            cache.remove(this.name);
            if(isDebug()){
                System.out.println(this + " shutdown");
            }
        } finally {
            mainLock.unlock();
        }
        this.unregisterMBean();
        MyFutureTask<V,T> task = null;
        while (null != (task = queue.poll())) {//关闭后打印未完成的任务
            task.cancel(false);
            System.err.println(String.format("Task [%s] NOT done.", task.getRealTask()));
        }
    }

    public void setStopExecutorTimeout(int stopExecutorTimeout) {
        this.stopExecutorTimeout = stopExecutorTimeout;
    }

    private int stopExecutorTimeout=3*1000;

    /*
    停止线程池，用于切换或者关闭线程池。
    特别注意：线程池关闭会等待stopExecutorTimeout毫秒，如果池内任务五分钟没有完结，依然会生成新的线程池来处理队列中的任务
     */
    public void stopExecutor() {
        mainLock.lock();
        try {

            if (PriorityTaskExecutor.this.isDebug()) {
                System.out.println(this + " stopExecutor");
            }
            executor.shutdown();
            try {
                executor.awaitTermination(stopExecutorTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                System.err.println("Error shutdown " + e +".Timeout "+stopExecutorTimeout);
            }
            executor = null;//关闭后设置executor为null，等待gc
        } finally {
            mainLock.unlock();
        }
    }

    static {

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                PriorityTaskExecutor.shutdownAll();
            }
        });
    }

    private void init() {
        mainLock.lock();
        try {
            executor = Executors.newFixedThreadPool(size);
            for (int i = 0; i < size; i++) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (!executor.isShutdown()) {
                            rateLimiterLock.readLock().lock();
                            try{
                                if(isRateLimiterEnabled()){
                                    rateLimiter.acquire();
                                }
                            }finally {
                                rateLimiterLock.readLock().unlock();
                            }
                            try {
                                MyFutureTask<V,T> task = queue.poll(1, TimeUnit.SECONDS);
                                if (task != null) {
                                    long c = count.incrementAndGet();
                                    if (PriorityTaskExecutor.this.isDebug()) {
                                        System.out.println(String.format("No.%s Task [%s] run on [%s]", c, task.getRealTask(), Thread.currentThread()));
                                    }
                                    task.run();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                });
            }
            status = RUNNING;
        } finally {
            mainLock.unlock();
        }
    }


    @Override
    public long getCount() {
        return count.longValue();
    }

    @Override
    public int getQueueSize() {
        return queue.size();
    }

    @Override
    public int getThreadPoolSize() {
        return this.size;
    }

    public void resize(int newSize) {
        if (newSize < 1||newSize==size) {
            return;
        }
        mainLock.lock();
        try {
            status = RESIZING;
            stopExecutor();
            size = newSize;
            init();
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public String printQueue() {
        MyFutureTask[] arr = queue.toArray(new MyFutureTask[0]);
        Arrays.sort(arr);
        StringBuffer sb = new StringBuffer();
        sb.append("QueueSize:").append(arr.length).append("\n");
        for (int i = 0; i < arr.length; i++) {
            sb.append(arr[i].getRealTask().toString()).append("\n");
        }

        return sb.toString();
    }

    public MyFutureTask<V,T > addTask(T task) {
        if (status != RUNNING && status != RESIZING) {
            throw new NotRunningException();
        }
        MyFutureTask<V,T> ft = new MyFutureTask<>(task);
        queue.add(ft);
        return ft;
    }


    public boolean cancelTask(MyFutureTask<V,T > task, boolean mayInterruptIfRunning) {
        queue.remove(task);
        return task.cancel(mayInterruptIfRunning);
    }

    public static class NotRunningException extends RuntimeException {

    }

    /*
     * 用法同 java.util.concurrent.FutureTask
     */
    public static class MyFutureTask<V,T  extends Task<V>> extends FutureTask implements Comparable<MyFutureTask> {
        public T getRealTask() {
            return task;
        }

        private T task;

        public MyFutureTask(T task) {
            super(task);
            this.task = task;
        }


        /*
                按优先级和添加时间进行排序。优先级越高，进入队列越早的task优先执行
                 */
        @Override
        public int compareTo(MyFutureTask o) {
            return this.task.compareTo(o.task);
        }
    }


    public static abstract class Task<V> implements Callable<V>, Comparable<Task> {

        private static class WrongPriorityException extends RuntimeException {
            WrongPriorityException(String message) {
                super(message);
            }
        }

        public int getPriority() {
            return priority;
        }

        public long getCreateTime() {
            return createTime;
        }

        public String getName() {
            return name;
        }

        private static final int max = 100;
        private static final int min = -100;

        public static final int HIGH = 0;
        public static final int LOW = 10;
        public static final int NORMAL = 5;

        public void setPriority(int priority) {
            this.priority = priority;
        }

        private int priority; //数字越小优先级越高
        private long createTime = new Date().getTime();
        private String name;


        public Task(String name, int priority) {
            super();
            this.name = name;
            if (priority > max || priority < min) {
                throw new WrongPriorityException(String.format("priority is [%s].But %s < p < %s", priority, min, max));
            }
            this.priority = priority;
        }

        @Override
        public abstract V call();

        @Override
        public String toString() {
            return String.format("name:%s priority:%s createTime:%s", name, this.priority, createTime);
        }

        @Override
        public int compareTo(Task o) {
            // 优先级不一致
            if (o.priority != this.priority) {
                return this.priority - o.priority;
            } else {
                long diff = this.createTime - o.createTime;
                if (diff > 0) {
                    return 1;
                } else if (diff == 0) {
                    return 0;
                } else {
                    return -1;
                }
            }
        }
    }

}
