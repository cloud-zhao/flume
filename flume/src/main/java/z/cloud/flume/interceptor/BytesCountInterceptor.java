package z.cloud.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Created by cloud on 17/12/27.
 */
public abstract class BytesCountInterceptor implements Interceptor{
    static final Logger logger = LoggerFactory.getLogger(BytesCountInterceptor.class);


    private long flushTimeout;
    private Queue<Long> queue = new ConcurrentLinkedDeque<Long>();
    private ScheduledExecutorService executorService;
    private ScheduledFuture<?> scheduledFuture;
    private ExecutorService executor;
    protected Long bytes=0L;
    protected Long count=0L;
    private boolean quit = false;

    protected BytesCountInterceptor(long flushTimeout){
        this.flushTimeout=flushTimeout;
    }

    public void initialize() {
        executorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(
                        "outputCountInfoToStore-" + Thread.currentThread().getId() + "-%d").build());
        executor = Executors.newSingleThreadExecutor();
        executor.submit(new Runnable() {
            public void run() {
                try{
                    countBytes();
                }catch (Exception e){
                    logger.error("count bytes failed.",e);
                }
            }
        });

        scheduledFuture = executorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    logger.info("flush output target.");
                    flushOutputTarget();
                }catch (Exception e){
                    logger.error("flushOutputTarget error.",e);
                }
            }
        },flushTimeout,flushTimeout,TimeUnit.MILLISECONDS);
    }


    protected void countBytes(){
        while(!quit){
            while(!queue.isEmpty()){
                bytes += queue.poll();
                count += 1L;
            }
        }
    }

    protected abstract void flushOutput();
    protected abstract void countClose();

    protected void flushOutputTarget(){
        synchronized (bytes) {
            flushOutput();
        }
    }
    protected void checkState(boolean flag,String msg){
        Preconditions.checkState(flag, msg);
    }

    public Event intercept(Event event) {
        int byteSize = event.getBody().length;
        queue.add((long)byteSize);
        return event;
    }


    public List<Event> intercept(List<Event> events) {
        for(Event event : events){
            intercept(event);
        }
        return events;
    }

    protected void executorClose(ExecutorService executorService){
        if(executorService != null){
            executorService.shutdown();
            while (executorService.isTerminated()){
                try {
                    executorService.awaitTermination(500, TimeUnit.MILLISECONDS);
                }catch (InterruptedException e){
                    logger.error("executorService awaitTermination failed.",e);
                }
            }
        }
    }
    protected void futureCancel(Future<?> future){
        if(future != null)
            future.cancel(true);
    }

    public void close() {
        quit = true;
        executorClose(executor);
        futureCancel(scheduledFuture);
        executorClose(executorService);

        countClose();
    }

    public static class Builder implements Interceptor.Builder {
        private String outputType;
        private long flushTimeout;
        private Context context;

        public Interceptor build() {
            logger.info("interceptor "+outputType+" mode.");
            if(outputType.equals("file")) {
                return new OutFileInterceptor(flushTimeout, context);
            }else if(outputType.equals("influx")){
                return new OutInfluxDBInterceptor(flushTimeout,context);
            }else{
                return new OutFileInterceptor(flushTimeout,context);
            }
        }

        public void configure(Context context) {
            this.context = context;
            outputType = context.getString(Constants.TYPE,
                    Constants.DEFAULT_TYPE);
            flushTimeout = context.getLong(Constants.FLUSH,
                    Constants.DEFAULT_FLUSH);
        }
    }

    private static class Constants {
        private static String TYPE="outputType";
        private static String DEFAULT_TYPE="file";

        private static String FLUSH="flushTimeout";
        private static long DEFAULT_FLUSH=3000L;
    }
}


