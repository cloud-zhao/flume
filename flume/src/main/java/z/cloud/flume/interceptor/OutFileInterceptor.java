package z.cloud.flume.interceptor;

import org.apache.flume.Context;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by cloud on 17/12/27.
 */
public class OutFileInterceptor extends BytesCountInterceptor{
    private String outputFile;
    private FileWriter fileWriter;

    OutFileInterceptor(long flushTimeout, Context context){
        super(flushTimeout);
        this.outputFile=context.getString(Constants.FILE);
        checkState(outputFile != null,"outputType if file the parameter outputFile must be specified.");
    }

    private FileWriter getFileWriter(){
        try{
            fileWriter = new FileWriter(outputFile,true);
            return fileWriter;
        }catch (IOException e){
            logger.error("Writer file "+outputFile+" error. ",e);
            return null;
        }
    }
    private void fileWriterClose(){
        if(fileWriter != null){
            try{
                fileWriter.close();
            }catch (IOException e){
                logger.error("file " + outputFile + " close error. ",e);
            }
        }
    }
    public void countClose(){
        fileWriterClose();
    }

    public void flushOutput(){
        if(fileWriter == null)
            fileWriter = getFileWriter();
        try {
            fileWriter.write("{\"bytes\":"+bytes+",\"count\":"+count+",\"timestamp\":"+System.currentTimeMillis()+"}\n");
            fileWriter.flush();
            bytes = 0L;
            count = 0L;
        } catch (IOException e) {
            logger.error("file writer failed.", e);
        }
    }

    private static class Constants {
        private static String FILE="outputFile";
    }

}
