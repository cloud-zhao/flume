package z.cloud.flume.source;


/**
 * Created by cloud on 17/11/10.
 */
public class CanalSourceConfigure {

    public static final String HOST_IP = "hostIp";
    public static final String DEFAULT_HOST_IP = "127.0.0.1";

    public static final String PORT = "port";
    public static final int DEFAULT_PORT = 11111;

    public static final String DESTINATION = "destination";
    public static final String DEFAULT_DESTINATION = "example";

    public static final String USERNAME = "userName";
    public static final String DEFAULT_USERNAME = "";

    public static final String PASSWORD = "password";
    public static final String DEFAULT_PASSWORD = "";

    public static final String SUBSCRIBE = "subscribe";
    public static final String DEFAULT_SUBSCRIBE = ".*\\..*";

    public static final String BATCHSIZE = "batchSize";
    public static final int DEFAULT_BATCHSIZE = 1000;

/*    public static final String BUFFERCOUNT = "bufferCount";
    public static final int DEFAULT_BUFFERCOUNT = 1000;

    public static final String FLUSHTIMEOUT = "flushTimeout";
    public static final long DEFAULT_FLUSHTIMEOUT = 3000L;*/

    public static final String SLEEPTIME = "sleepTime";
    public static final long DEFAULT_SLEEPTIME = 1000L;

    public static final String CHARSET = "charset";
    public static final String DEFAULT_CHARSET = "UTF-8";
}
