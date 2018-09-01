package z.cloud.flume.interceptor;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
  * Created by cloud on 18/8/30.
  */
public class BCInterceptor implements Interceptor{
    private final Logger logger = LoggerFactory.getLogger(BCInterceptor.class);

    private Long bytes = 0L;
    private Long count = 0L;
    private final Object lock = new Object();
    private Long totalBytes = 0L;
    private Long totalCount = 0L;
    private HttpClient httpClient;

    private BCInterceptorConfig bcic;
    private ScheduledExecutorService executorService;
    private ScheduledFuture<?> scheduledFuture;

    private BCInterceptor(BCInterceptorConfig bcic) {
        this.bcic = bcic;
    }

    private HttpClient getHttpClient() {
        if (null == httpClient)
            httpClient = new DefaultHttpClient();
        return httpClient;
    }

    public void initialize() {
        executorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("BCInterceptor-%d").build());
        scheduledFuture = executorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    send(getHttpClient());
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                }
            }
        },bcic.flushTimeout,bcic.flushTimeout, TimeUnit.MILLISECONDS);
    }

    public Event intercept(Event event) {
        if(event.getBody().length <= bcic.dataBytes) {
            synchronized (lock) {
                bytes += event.getBody().length;
                count += 1;
            }
            return event;
        }
        return null;
    }

    public List<Event> intercept(List<Event> events) {
        List<Event> eventList = Lists.newArrayListWithCapacity(events.size());
        for (Event event: events){
            Event newEvent = intercept(event);
            if (null != newEvent)
                eventList.add(newEvent);
        }
        return eventList;
    }

    public void close() {
        if (null != scheduledFuture)
            scheduledFuture.cancel(true);
        if (null != executorService) {
            executorService.shutdown();
            while (executorService.isTerminated()) {
                try {
                    executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    private void send(HttpClient httpClient) {
        Long bytes;
        Long count;
        synchronized (lock) {
            bytes=this.bytes - totalBytes;
            count=this.count - totalCount;
        }
        String url = bcic.url+"?topic="+bcic.topic+"&bytes="+bytes+"&count="+count;
        HttpGet httpGet = new HttpGet(url);
        try {
            logger.info("send request "+url);
            HttpResponse httpResponse = httpClient.execute(httpGet);
            if(httpResponse.getStatusLine().getStatusCode() == 200) {
                logger.info("send bytes success.");
                totalBytes += bytes;
                totalCount += count;
            } else {
                logger.warn("send bytes warning. " + httpResponse.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }

    public static class Builder implements Interceptor.Builder {
        private String topic;
        private String url;
        private Long flushTimeout;
        private Long dataBytes;

        public Interceptor build() {
            return new BCInterceptor(new BCInterceptorConfig(url,topic,flushTimeout,dataBytes));
        }

        public void configure(Context context) {
            url = context.getString(BCInterceptorConfig.URL);
            topic = context.getString(BCInterceptorConfig.TOPIC);
            flushTimeout = context.getLong(BCInterceptorConfig.FLUSH_TIME, 3000L);
            dataBytes = context.getLong(BCInterceptorConfig.DATA_BYTES, 3*1024*1024L);
        }
    }

}

class BCInterceptorConfig {
    static String FLUSH_TIME = "flushTimeout";
    static String DATA_BYTES = "dataBytes";
    static String URL = "url";
    static String TOPIC = "topic";

    Long flushTimeout = 0L;
    Long dataBytes = 0L;
    String url;
    String topic;

    BCInterceptorConfig(String url, String topic, Long flushTimeout, Long dataBytes) {
        this.url = url;
        this.topic = topic;
        this.flushTimeout = flushTimeout;
        this.dataBytes = dataBytes;
    }
}
