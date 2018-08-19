package z.cloud.flume.source;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.google.gson.Gson;
import com.google.gson.LongSerializationPolicy;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by cloud on 17/11/9.
 */
public class CanalSource extends AbstractSource implements Configurable,EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(CanalSource.class);

    private String hostIp;
    private int port;
    private String destination;
    private String username;
    private String password;
    private String subscribe;
    private int batchSize;
    private long sleepTime;
    private String charset;
    //private ChannelProcessor channelProcessor;
    private SourceCounter sourceCounter;

    private ExecutorService executorService;
    private Future<?> canalSimpleClientFuture;
    private CanalSimpleClient canalSimpleClient;


    public void configure(Context context){
        hostIp = context.getString(CanalSourceConfigure.HOST_IP,
                CanalSourceConfigure.DEFAULT_HOST_IP);
        port = context.getInteger(CanalSourceConfigure.PORT,
                CanalSourceConfigure.DEFAULT_PORT);
        destination = context.getString(CanalSourceConfigure.DESTINATION,
                CanalSourceConfigure.DEFAULT_DESTINATION);
        username = context.getString(CanalSourceConfigure.USERNAME,
                CanalSourceConfigure.DEFAULT_USERNAME);
        password = context.getString(CanalSourceConfigure.PASSWORD,
                CanalSourceConfigure.DEFAULT_PASSWORD);
        subscribe = context.getString(CanalSourceConfigure.SUBSCRIBE,
                CanalSourceConfigure.DEFAULT_SUBSCRIBE);
        sleepTime = context.getLong(CanalSourceConfigure.SLEEPTIME,
                CanalSourceConfigure.DEFAULT_SLEEPTIME);
        batchSize = context.getInteger(CanalSourceConfigure.BATCHSIZE,
                CanalSourceConfigure.DEFAULT_BATCHSIZE);
        charset = context.getString(CanalSourceConfigure.CHARSET,
                CanalSourceConfigure.DEFAULT_CHARSET);

        if(sourceCounter == null){
            sourceCounter = new SourceCounter(getName());
        }
        /*if(channelProcessor == null)
            channelProcessor = getChannelProcessor();*/
    }

    @Override
    public void start(){
        logger.info("CanaSource start begin.");

        logger.debug("init executorService single thread.");
        executorService = Executors.newSingleThreadExecutor();
        logger.debug("init CanalSimpleClient.");
        canalSimpleClient = new CanalSimpleClient(
                hostIp, port,
                destination,username,password,subscribe,charset,
                batchSize,sleepTime,
                getChannelProcessor(),sourceCounter);
        logger.debug("submit canalSimpleClient to executorService");
        canalSimpleClientFuture = executorService.submit(canalSimpleClient);

        logger.debug("sourceCounter start.");
        sourceCounter.start();
        super.start();

        logger.info("CanalSource start end.");
    }

    @Override
    public void stop(){

        logger.debug("stop canalSimpleClient");
        if(canalSimpleClient != null)
            canalSimpleClient.stop();

        logger.debug("stop canalSimpleClientFuture");
        if(canalSimpleClientFuture != null){
            canalSimpleClientFuture.cancel(true);
        }

        logger.debug("stop executorService");
        executorService.shutdown();
        while (!executorService.isTerminated()){
            logger.debug("Waiting for exec executor service to stop");
            try{
                executorService.awaitTermination(500L, TimeUnit.MILLISECONDS);
            }catch (InterruptedException e){
                logger.error("Interrupted while waiting for exec executor service to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }

    }


}

class CanalSimpleClient implements Runnable {

    private String hostIp;
    private int port;
    private String destination;
    private String username;
    private String password;
    private final int batchSize;
    private String subscribe;
    private String charset;
    private boolean quit = false;
    private long sleepTime;
    private final ChannelProcessor channelProcessor;
    private final SourceCounter sourceCounter;
    //private SystemClock systemClock = new SystemClock();
    private static final Logger log = LoggerFactory.getLogger(CanalSimpleClient.class);

    public CanalSimpleClient(ChannelProcessor channelProcessor,
                             SourceCounter sourceCounter){
        this(
                AddressUtils.getHostIp(),11111,"example",
                "","",".*\\..*","UTF-8",
                1200,1000,
                channelProcessor,sourceCounter);
    }
    public CanalSimpleClient(String hostIp,
                             int port,
                             String destination,
                             String username,
                             String password,
                             String subscribe,
                             String charset,
                             int batchSize,
                             long sleepTime,
                             ChannelProcessor channelProcessor,
                             SourceCounter sourceCounter){
        this.batchSize=batchSize;
        this.subscribe=subscribe;
        this.port=port;
        this.hostIp=hostIp;
        this.destination=destination;
        this.username=username;
        this.password=password;
        this.charset=charset;
        this.sleepTime=sleepTime;
        this.channelProcessor=channelProcessor;
        this.sourceCounter=sourceCounter;
    }

    public void stop(){
        this.quit=true;
    }

    private void pushEventBatch(List<Event> eventList,CanalConnector connector,long batchId){
        try {
            log.debug("pushEventBatch push event list " + eventList.size());
            channelProcessor.processEventBatch(eventList);
            sourceCounter.addToEventAcceptedCount(eventList.size());
            connector.ack(batchId);
        }catch (Exception e){
            connector.rollback(batchId);
            int eventLength=0;
            for(Event se : eventList){
                eventLength+=se.getBody().length;
            }
            log.error("Push eventBatch error,Event Size : "+eventLength,e);
        }finally {
            eventList.clear();
        }
    }

    private void addEventList(List<Event> eventList,Map<String,Object> jsonMap,Gson gson){
        synchronized (eventList) {
            log.debug("add event to event list and push event");
            sourceCounter.incrementEventReceivedCount();
            Map<String,String> eventHeader = new HashMap<String, String>();
            String dataBase = (String)jsonMap.get("DATABASE");
            String tableName = (String)jsonMap.get("TABLE");
            eventHeader.put("key",dataBase+"-"+tableName);
            eventList.add(EventBuilder.withBody(gson.toJson(jsonMap),Charset.forName(charset),eventHeader));
        }
    }

    public void run(){
        do {
            final List<Event> eventList = new ArrayList<Event>();
            CanalConnector connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(hostIp, port),
                    destination, username, password);
            Gson gson = new Gson();
            try {
                connector.connect();
                connector.subscribe(subscribe);
                connector.rollback();
                while (!quit) {
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 && size == 0) {
                        log.info("Fetch message 0. thread sleep " + sleepTime + "ms.");
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            log.error("Thread sleep 1s error. ", e);
                        }
                    } else {
                        log.debug("printEntry message.getEntries()");
                        printEntry(message.getEntries(), gson, eventList);
                        synchronized (eventList) {
                            if(!eventList.isEmpty()){
                                pushEventBatch(eventList, connector, batchId);
                            }else {
                                connector.ack(batchId);
                            }
                        }
                    }
                    //connector.ack(batchId);
                    //connector.rollback(batchId);
                }
            } catch (Exception e) {
                log.error("Canal connector error. ", e);
            } finally {
                connector.disconnect();
            }
        }while(! quit);
    }

    private void printEntry(List<Entry> entrys, Gson gson,List<Event> eventList){
        log.debug("foreach entrys");
        for(Entry entry : entrys){
            if(entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND){
                log.debug("skip entryType.TRANSACTIONBEGIN and entryType.TRANSACTIONEND");
                continue;
            }
            RowChange rowChange = null;
            try{
                log.debug("init rowChange");
                rowChange=RowChange.parseFrom(entry.getStoreValue());
            }catch (InvalidProtocolBufferException e){
                log.error("entry string : "  + entry.toString() + " error : " + e);
            }

            log.debug("init eventType");
            EventType eventType = rowChange.getEventType();

            log.debug("init headerMap");
            Map<String,Object> hm = headerMap(entry,eventType);
            for(RowData rowData : rowChange.getRowDatasList()){
                addEventList(eventList,jsonMap(eventType,rowData,hm),gson);
            }
        }
    }

    private Map<String,Object> tableMap(List<Column> cs){
        Map<String,Object> tm = new HashMap<String,Object>();
        for(int i=0;i<cs.size();i++){
            tm.put(cs.get(i).getName(),cs.get(i).getValue());
        }
        return tm;
    }

    private Map<String,Object> headerMap(Entry entry,EventType eventType){
        Map<String,Object> hm = new HashMap<String,Object>();

        String database = entry.getHeader().getSchemaName();
        String table = entry.getHeader().getTableName();

        hm.put("DATABASE",database);
        hm.put("GLOBAL_ID",database+"-"+table+"-0-"+CanalSourceUtil.getGlobalId()+"-0");
        hm.put("EVENT_SERVER_ID","0x002");
        hm.put("BINLOG_NAME",entry.getHeader().getLogfileName());
        hm.put("BINLOG_POS",entry.getHeader().getLogfileOffset());
        hm.put("TIME",CanalSourceUtil.myTime());
        hm.put("TABLE",table);
        hm.put("GROUP_ID","0x001");
        if(eventType == EventType.INSERT) {
            hm.put("TYPE","I");
        }else if(eventType == EventType.UPDATE){
            hm.put("TYPE","U");
        }else if(eventType == EventType.DELETE){
            hm.put("TYPE","D");
        }
        return hm;
    }

    private Map<String,Object> jsonMap(EventType eventType,RowData rowData,Map<String,Object> hm){
        log.debug("init tableMap. and create event type json");
        if(eventType == EventType.INSERT) {
            log.debug("init tableMap. and create INSERT json");
            Map<String, Object> tm = tableMap(rowData.getAfterColumnsList());
            hm.put("NEW_VALUES", tm);
        }else if(eventType == EventType.UPDATE){
            log.debug("init tableMap. and create UPDATE json");
            Map<String, Object> tma = tableMap(rowData.getAfterColumnsList());
            Map<String, Object> tmo = tableMap(rowData.getBeforeColumnsList());
            hm.put("NEW_VALUES",tma);
            hm.put("OLD_VALUES",tmo);
        }else if(eventType == EventType.DELETE){
            log.debug("init tableMap. and create DELETE json");
            Map<String, Object> tm = tableMap(rowData.getBeforeColumnsList());
            hm.put("OLD_VALUES",tm);
        }
        return hm;
    }
}

class CanalSourceUtil {
    public static String myTime(){
        return myTime("yyyyMMddHHmmss");
    }
    public static String myTime(String format){
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(currentTime);
    }

    public static String getGlobalId(){
        int max=999999998;
        int min=100000000;
        Random random = new Random();

        int s = random.nextInt(max)%(max-min+1) + min;
        return String.valueOf(s);
    }
}