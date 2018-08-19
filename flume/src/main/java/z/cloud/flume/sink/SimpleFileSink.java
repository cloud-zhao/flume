package z.cloud.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.FlumeConfigurationError;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by cloud on 17/11/11.
 */
public class SimpleFileSink extends AbstractSink implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(SimpleFileSink.class);

    private String fileName;
    private int batchSize;
    private String charset;

    public void configure(Context context){
        fileName = context.getString("fileName","/home/zhaozhen01/install/flume_canal_test/canal_sink.txt");
        batchSize = context.getInteger("batchSize",1000);
        charset = context.getString("charset","UTF-8");
    }

    @Override
    public void start(){
        //null
    }

    @Override
    public void stop(){
        //null
    }

    public Status process() throws EventDeliveryException{
        Status status = Status.READY;
        Event event;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        FileOutputStream fileOutputStream = null;

        try{
            fileOutputStream = new FileOutputStream(fileName, true);
        }catch (FileNotFoundException e){
            log.error(fileName + " not fount", e);
        }
        try{
            transaction.begin();
            for(int i=0;i<=batchSize;i++) {
                event = channel.take();
                if(event != null) {
                    String body = new String(event.getBody());
                    body += "\n";
                    if(fileOutputStream != null) {
                        try {
                            fileOutputStream.write(body.getBytes(charset));
                        } catch (IOException e) {
                            log.error(fileName + "write " + body + "failed.", e);
                        }
                    }
                }else {
                    status = Status.BACKOFF;
                }
            }
            transaction.commit();
        }catch (Exception e){
            transaction.rollback();
            status = Status.BACKOFF;
            log.error("simpleFileSink error.",e);
            throw new EventDeliveryException("simpleFileSink error ",e);
        }finally {
            transaction.close();
        }

        if(fileOutputStream != null){
            try{
                fileOutputStream.close();
            }catch (IOException e){
                log.error(fileName + " close failed .",e);
            }
        }

        return status;
    }
}
