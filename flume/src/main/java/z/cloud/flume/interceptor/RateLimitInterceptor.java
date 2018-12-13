package z.cloud.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
  * Created by cloud on 18/12/13.
  */
public class RateLimitInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(RateLimitInterceptor.class);

    //速率是秒计算，所以perTime为1s，保证精确用纳秒计算
    private static final long PERTIME = 1000000000;

    //计数器
    private static AtomicInteger number = new AtomicInteger(0);
    private long limitRate;
    private long headerSize;

    //每次已发送长度
    private long sendLength;
    //每次发送开始时间
    private long lastTick;

    private RateLimitInterceptor(long limitRate, long headerSize) {
        this.limitRate = limitRate;
        this.headerSize = headerSize;
        this.sendLength = 0;
        this.lastTick = System.nanoTime();
    }

    @Override
    public void initialize() {}

    @Override
    public Event intercept(Event event) {
        number.getAndIncrement();
        // 每当发送长度大于单位时间总长度limitRate * 1s 时候进行判断，超速则sleep相应时间
        if(sendLength > limitRate) {
            //System.nanoTime() - lastTick为发送时间区间，按照速率计算实际发送长度如果大于应发长度则需要限速sleep超速的时间
            //即 sendLength > limitRate * (System.nanoTime() - lastTick) / PERTIME
            long missTime = sendLength / limitRate * PERTIME  - (System.nanoTime() - lastTick);
            if(missTime > 0) {
                try {
                    logger.info(String.format("Limit source send rate exceed, Header Length:%d, " +
                            "Sent Length:%d, Last Tick:%d, Sleep Time:%d ms, Number:%d\n", new Object[] {
                            headerSize, sendLength, lastTick, missTime / 1000000, number.get()} ));
                    TimeUnit.MILLISECONDS.sleep(missTime / 1000000);
                    TimeUnit.NANOSECONDS.sleep(missTime % 1000000);
                } catch (InterruptedException e) {
                    logger.warn("TimeUnit sleep Exception in rate limit com.flume.homework.interceptor", e);
                }
            }
            //重置计数器，发送长度以及重新开始计时
            number.set(0);
            sendLength = 0;
            lastTick = System.nanoTime();
        } else {
            //没有超出速率，持续累加已发送长度，直接返回event
            sendLength += headerSize + event.getBody().length;
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for(Event event: list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {}

    public static class Builder implements Interceptor.Builder {
        private long limitRate;
        private long headerSize;

        @Override
        public Interceptor build() {
            return new RateLimitInterceptor(limitRate, headerSize);
        }

        @Override
        public void configure(Context context) {
            limitRate = context.getLong(Constants.LIMIT_RATE, Constants.LIMIT_RATE_DFLT);
            headerSize = context.getLong(Constants.HEADER_SIZE, Constants.HEADER_SIZE_DELT);
        }
    }

    public static class Constants {
        public static final String LIMIT_RATE = "limit";
        //默认限速2MB/s
        public static final long LIMIT_RATE_DFLT = 2048000;
        public static final String HEADER_SIZE = "headerSize";
        //默认头部size为0
        public static final long HEADER_SIZE_DELT = 0;
    }
}
