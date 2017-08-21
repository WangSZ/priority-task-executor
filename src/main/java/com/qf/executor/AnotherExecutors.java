package com.qf.executor;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 版权：Copyright 2017 QuarkFinance IT
 * 描述：在Executors的基础上添加JMX监控和优先级队列
 * 创建人：ShaozeWang15
 * 创建时间：2017/8/
 * 备注:
 */
public class AnotherExecutors {

    private final static String PREFIX = "TrackingThreadPool:type=";

    public static TrackingThreadPool registerMBean(TrackingThreadPool pool) {
        try {
            ObjectName objectName = new ObjectName(PREFIX + pool.getName());
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(new ThreadPoolStatus(pool), objectName);
        } catch (JMException e) {
            // ignore
            e.printStackTrace();
        }
        return pool;
    }

    public static TrackingThreadPool unregisterMBean(TrackingThreadPool pool) {
        try {
            ObjectName objectName = new ObjectName(PREFIX + pool.getName());
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(objectName);
        } catch (JMException e) {
            // ignore
            e.printStackTrace();
        }
        return pool;
    }
    public static TrackingThreadPool setFactory(TrackingThreadPool pool){
        pool.setThreadFactory(new TrackingThreadFactory(pool.getName()));
        return pool;
    }
    /*
    替换Executors.newFixedThreadPool
     */
    public static ExecutorService newFixedThreadPool(String name,int nThreads) {
        return registerMBean(setFactory(new TrackingThreadPool(name,nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>())));
    }

    /*
    替换Executors.newFixedThreadPool。队列大小受限。当超出能力时，异步转同步调用
     */
    public static ExecutorService newFixedThreadPoolWithQueueSize(String name,int nThreads,int queueSize) {
        TrackingThreadPool pool = registerMBean(setFactory(new TrackingThreadPool(name, nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueSize))));
        pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return pool;
    }

    /*
    在Executors.newFixedThreadPool基础上添加优先级队列
     */
    public static ExecutorService newFixedThreadPoolWithPriorityQueue(String name,int nThreads) {
        return registerMBean(setFactory(new TrackingThreadPool(name,nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue<Runnable>(11))));
    }

    /*
    适合执行时间短，需要延时小，大量异步任务的场景。比如三方接口调用。但是如果三方接口延时很大，
    预估可能造成大量积压的话，可以考虑设置合适的maximumPoolSize，来防止创建大量线程最后导致雪崩。
     */
    public static ExecutorService newCachedThreadPool(String name,int maximumPoolSize) {
        return registerMBean(setFactory(new TrackingThreadPool(name,0, maximumPoolSize,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>())));
    }

    public static ExecutorService newCachedThreadPool(String name) {
        return newCachedThreadPool(name,Integer.MAX_VALUE);
    }


    public interface ThreadPoolStatusMBean {
        public int getActiveThreads();

        public int getActiveTasks();

        public long getTotalTasks();

        public int getQueuedTasks();

        public double getAverageTaskTime();

        public String[] getActiveTaskNames();

        public String[] getQueuedTaskNames();

        public void resume();
        public void pause();
        public void setCorePoolSize(int corePoolSize);
        public int getCorePoolSize();
        public int getMaximumPoolSize();
        public void setMaximumPoolSize(int maximumPoolSize);
        public void setKeepAliveTime(long time);
        public long getKeepAliveTime();
        public boolean isPaused();
        public String getQueueClassName();
        public String[] getErrors();
    }

    public static class ThreadPoolStatus implements ThreadPoolStatusMBean {
        private final TrackingThreadPool pool;

        public ThreadPoolStatus(TrackingThreadPool pool) {
            this.pool = pool;
        }

        public int getActiveThreads() {
            return pool.getPoolSize();
        }

        public int getActiveTasks() {
            return pool.getActiveCount();
        }

        public long getTotalTasks() {
            return pool.getTotalTasks();
        }

        public int getQueuedTasks() {
            return pool.getQueue().size();
        }

        public double getAverageTaskTime() {
            return pool.getAverageTaskTime();
        }

        public String[] getActiveTaskNames() {
            return toStringArray(pool.getInProgressTasks());
        }

        public String[] getQueuedTaskNames() {
            return toStringArray(pool.getQueue());
        }

        @Override
        public void resume() {
            pool.resume();
        }

        @Override
        public void setCorePoolSize(int corePoolSize){
            checkSetCorePoolSize();
            pool.setCorePoolSize(corePoolSize);
        }

        @Override
        public int getCorePoolSize() {
            return pool.getCorePoolSize();
        }
        @Override
        public void setMaximumPoolSize(int maximumPoolSize) {
            checkSetMaximumPoolSize();
            pool.setMaximumPoolSize(maximumPoolSize);
        }

        @Override
        public void setKeepAliveTime(long time){
            checkSetKeepAliveTime();
            pool.setKeepAliveTime(time, TimeUnit.MILLISECONDS);
        }

        private void checkSetKeepAliveTime() {
            if(pool.getQueue() instanceof  SynchronousQueue){
                throw  new UnsupportedOperationException();
            }
        }
        private void checkSetCorePoolSize() {
            if(pool.getQueue() instanceof  SynchronousQueue){
                throw  new UnsupportedOperationException();
            }
        }
        private void checkSetMaximumPoolSize(){
            if(pool.getQueue() instanceof  SynchronousQueue){
                throw  new UnsupportedOperationException();
            }
        }

        @Override
        public long getKeepAliveTime(){
            return pool.getKeepAliveTime( TimeUnit.MILLISECONDS);
        }

        @Override
        public String[] getErrors (){
            ArrayList<Map.Entry<String, AtomicLong>> entries=new ArrayList<>( pool.getErrorCountMap().entrySet());
            Map.Entry<String, AtomicLong>[] entriesArray = entries.toArray(new Map.Entry[0]);
            Arrays.sort(entriesArray, new Comparator<Map.Entry<String, AtomicLong>>() {
                @Override
                public int compare(Map.Entry<String, AtomicLong> a, Map.Entry<String, AtomicLong> b) {
                    if( a.getValue().longValue()>b.getValue().longValue()){
                        return -1;
                    }else if( a.getValue().longValue()<b.getValue().longValue()){
                        return 1;
                    }
                    return a.getKey().compareTo(b.getKey());
                }
            });
            String[] arr=new String[entriesArray.length];
            for (int i = 0; i < arr.length; i++) {
                arr[i]=entriesArray[i].toString();
            }
            return arr;
        }

        @Override
        public int getMaximumPoolSize(){
            return pool.getMaximumPoolSize();
        }
        @Override
        public void pause() {
            pool.pause();
        }
        @Override
        public boolean isPaused() {

            return pool.isPaused;
        }

        @Override
        public String getQueueClassName(){
            return  pool.getQueue().getClass().toString();
        }

        private String[] toStringArray(Collection<Runnable> collection) {
            Object[] arr = collection.toArray();
            if (collection instanceof PriorityBlockingQueue) {
                Arrays.sort(arr);
            }
            ArrayList<String> list = new ArrayList<String>();
            for (int i = 0; i < arr.length; i++) {
                list.add(arr[i].toString());
            }
            return list.toArray(new String[0]);
        }
    }
    private static class TrackingThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        TrackingThreadFactory(String name) {
            SecurityManager var1 = System.getSecurityManager();
            this.group = var1 != null ? var1.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = name+"-thread-";
        }

        public Thread newThread(Runnable var1) {
            Thread var2 = new Thread(this.group, var1, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
            if (var2.isDaemon()) {
                var2.setDaemon(false);
            }

            if (var2.getPriority() != 5) {
                var2.setPriority(5);
            }

            return var2;
        }
    }
    private final static class TrackingThreadPool extends ThreadPoolExecutor {

        private final static AtomicLong cc=new AtomicLong();

        public TrackingThreadPool(String name,int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
            cc.incrementAndGet();
            this.name=name;
            if(this.name==null){
                this.name="TrackingThreadPool-"+cc.toString();
            }
        }

        public String getName() {
            return name;
        }

        private String name;

        private final ConcurrentHashMap<Runnable, Boolean> inProgress = new ConcurrentHashMap<Runnable, Boolean>();

        public ConcurrentHashMap<String, AtomicLong> getErrorCountMap() {
            return errorCountMap;
        }

        private final ConcurrentHashMap<String,AtomicLong> errorCountMap=new ConcurrentHashMap<>();
        private final ThreadLocal<Long> startTime = new ThreadLocal<Long>();
        private AtomicLong totalTime = new AtomicLong();
        private AtomicLong totalTasks = new AtomicLong();

        public Set<Runnable> getInProgressTasks() {
            return Collections.unmodifiableSet(inProgress.keySet());
        }

        public long getTotalTasks() {
            return totalTasks.longValue();
        }

        public double getAverageTaskTime() {
            return (totalTasks.intValue() == 0) ? 0 : totalTime.longValue() / totalTasks.longValue();
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
            if (this.getQueue() instanceof PriorityBlockingQueue) {
                if (callable instanceof Task) {
                    return new MyFutureTask<>((Task) callable);
                }
                if (callable instanceof Comparable) {
                    return new MyFutureTask<>(new CallableAdapterTask<T>(callable));
                }
            }
            return super.newTaskFor(callable);
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
            if (this.getQueue() instanceof PriorityBlockingQueue) {
                if (runnable instanceof Task) {
                    return new MyFutureTask<>((Task) runnable);
                }
                if (runnable instanceof Comparable) {
                    return new MyFutureTask<>(new RunnableAdapterTask<>(runnable, value));
                }
            }
            return super.newTaskFor(runnable, value);
        }

        private AtomicLong getErrorCounter(String name){
            AtomicLong c=errorCountMap.get(name);
            if(c==null){
                synchronized (errorCountMap){
                    if(errorCountMap.containsKey(name)){
                        c=errorCountMap.get(name);
                    }else {
                        c = errorCountMap.put(name, new AtomicLong(0));
                    }
                }
            }
            return c;
        }

        private boolean isPaused;
        private ReentrantReadWriteLock pauseLock = new ReentrantReadWriteLock();
        private Condition unpaused = pauseLock.writeLock().newCondition();

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            long time = System.currentTimeMillis() - startTime.get().longValue();
            totalTime.addAndGet(time);
            totalTasks.incrementAndGet();
            inProgress.remove(r);
            if(t!=null){
                String msg=t.getMessage();
                if(msg==null){
                    msg=t.getLocalizedMessage();
                }
                if(msg==null){
                    msg=t.toString();
                }
                if(msg==null){
                    msg=t.getClass().toString();
                }
                getErrorCounter(msg).incrementAndGet();
            }
            super.afterExecute(r, t);
        }

        @Override
        protected void beforeExecute(Thread t, Runnable r) {
            super.beforeExecute(t, r);
            pauseLock.readLock().lock();
            try {
                if (isPaused) {//如果暂停，则升级到写锁再等待
                    pauseLock.readLock().unlock();
                    pauseLock.writeLock().lock();
                    try {
                        while (isPaused) {
                            unpaused.await();
                        }
                    } catch (InterruptedException ie) {
                        t.interrupt();
                    } finally {
                        pauseLock.writeLock().unlock();
                        pauseLock.readLock().lock();
                    }
                }
            } finally {
                pauseLock.readLock().unlock();
            }
            inProgress.put(r, Boolean.TRUE);
            startTime.set(new Long(System.currentTimeMillis()));
        }

        public void pause() {
            pauseLock.writeLock().lock();
            try {
                isPaused = true;
            } finally {
                pauseLock.writeLock().unlock();
            }
        }

        public void resume() {
            pauseLock.writeLock().lock();
            try {
                isPaused = false;
                unpaused.signalAll();
            } finally {
                pauseLock.writeLock().unlock();
            }
        }

        @Override
        protected void terminated() {
            AnotherExecutors.unregisterMBean(this);
            super.terminated();
        }
    }


    /*
     * 用法同 java.util.concurrent.FutureTask
     */
    public static class MyFutureTask<V, T extends Task<V>> extends FutureTask implements Comparable<MyFutureTask> {
        public T getRealTask() {
            return task;
        }

        private T task;

        private MyFutureTask(T task) {
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

    private static class CallableAdapterTask<V> extends Task {
        private Callable<V> callable;

        public CallableAdapterTask(Callable<V> callable) {
            this.callable = callable;
        }

        @Override
        public Object call() throws Exception {
            return callable.call();
        }

        @Override
        public int compareTo(Task o) {
            if (o instanceof CallableAdapterTask && this.callable instanceof Comparable) {
                CallableAdapterTask oo = (CallableAdapterTask) o;
                return ((Comparable) this.callable).compareTo(oo.callable);
            }
            return super.compareTo(o);
        }
    }

    private static class RunnableAdapterTask<V> extends Task {
        final Runnable runnable;
        final V result;

        public RunnableAdapterTask(Runnable runnable, V result) {
            this.runnable = runnable;
            this.result = result;
        }

        @Override
        public Object call() throws Exception {
            runnable.run();
            return result;
        }

        @Override
        public int compareTo(Task o) {
            if (o instanceof RunnableAdapterTask && this.runnable instanceof Comparable) {
                RunnableAdapterTask oo = (RunnableAdapterTask) o;
                return ((Comparable) this.runnable).compareTo(oo.runnable);
            }
            return super.compareTo(o);
        }
    }

    public static abstract class Task<V> implements Callable<V>, Comparable<Task> {

        public static class WrongPriorityException extends RuntimeException {
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
        private long createTime;
        private String name;

        public Task() {
            this("TASK");
            this.name = String.format("%s-%s", this.getName(), this.getCreateTime());
        }

        public Task(String name) {
            this(name, NORMAL);
        }

        public Task(String name, int priority) {
            this(name, priority, System.currentTimeMillis());
        }

        public Task(String name, int priority, long createTime) {
            super();
            this.name = name;
            if (priority > max || priority < min) {
                throw new PriorityTaskExecutor.Task.WrongPriorityException(String.format("priority is [%s].But %s < p < %s", priority, min, max));
            }
            this.priority = priority;
            this.createTime = createTime;
        }

        @Override
        public abstract V call() throws Exception;

        @Override
        public String toString() {
            return String.format("name:%s priority:%s createTime:%s", getName(), this.getPriority(), getCreateTime());
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