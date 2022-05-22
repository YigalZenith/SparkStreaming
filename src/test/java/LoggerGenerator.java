import org.apache.flume.FlumeException;
import org.apache.log4j.Logger;

/**
 * 日志产生模拟
 */
public class LoggerGenerator {
    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            logger.info("a b c");
            Thread.sleep(1000);
        }
    }
}
