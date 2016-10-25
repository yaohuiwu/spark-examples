package org.example.spark.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 * <p>
 *     to run the app on the cluster:
 *     <p>
 *         first, put README.md and spark-1.0-SNAPSHOT.jar to hdfs /tmp dir.
 *     </p>
 *     <p>
 *         then:<br/>
 *         $SPARK_HOME/bin/spark-submit \<br/>
 *          --class "org.example.spark.spark.SimpleApp" \ <br/>
 *          --master spark://master:6066 --deploy-mode cluster --supervise \ <br/>
 *          hdfs://master:9000/tmp/spark-1.0-SNAPSHOT.jar \ <br/>
 *          hdfs://master:9000/tmp/README.md
 *     </p>
 * </p>
 *
 */
public class SimpleApp
{

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleApp.class);

    public static void main( String[] args )
    {
        String logFile = "hdfs://192.168.9.19:9000/tmp/README.md"; // Should be some file on your system
        if(args != null && args.length > 0){
            logFile = args[0];
        }

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        LOGGER.info("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.close();
    }
}
