package org.example;

import org.apache.hadoop.shaded.org.checkerframework.checker.nullness.qual.Raw;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

/**
 * Hello world!
 *
 */
public class HadoopReading {
    public static void main( String[] args ) {
        System.setProperty("hadoop.home.dir","/Users/barissss/Desktop/hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().master("local").appName("SparkHDFSExample").getOrCreate();
        Dataset<Row> rawData = sparkSession.read().option("header",true).csv("hdfs://localhost:8020/data/movies.csv");
        Dataset<Row> twoThousandDS = rawData.filter(rawData.col("title").contains("2000"));
        twoThousandDS.show();


        twoThousandDS.write().csv("hdfs://localhost:8020/data/twoThousand"); //writing the csv content to twoThousand folder.









    }
}
