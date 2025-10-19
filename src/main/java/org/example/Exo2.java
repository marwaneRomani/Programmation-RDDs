package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Exo2 {
    
    // Regex based on provided format
    // IP - user [date] "METHOD URL PROTOCOL" code size "referer" "agent"
    private static final String LOG_PATTERN = "^(\\S+) \\S+ \\S+ \\[(.+?)\\] \"(\\S+) (\\S+) \\S+\" (\\d{3}) (\\d+) .*";
    private static final Pattern PATTERN = Pattern.compile(LOG_PATTERN);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LogAnalysis").setMaster("local[*]");
        // Reduce log noise
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // Set log level to WARN to avoid cluttering output
            sc.setLogLevel("WARN");

            JavaRDD<String> lines = sc.textFile("access.log");

            // Extract fields
            JavaRDD<LogEntry> logs = lines.map(Exo2::parseLine).filter(entry -> entry != null);
            logs.cache(); // Cache for multiple actions

            // 3. Stats
            long totalRequests = logs.count();
            long totalErrors = logs.filter(log -> log.code >= 400).count();
            double errorPercentage = totalRequests > 0 ? (double) totalErrors / totalRequests * 100 : 0;

            System.out.println("=== 3. Statistiques de base ===");
            System.out.println("Nombre total de requêtes : " + totalRequests);
            System.out.println("Nombre total d'erreurs : " + totalErrors);
            System.out.printf("Pourcentage d'erreurs : %.2f%%\n", errorPercentage);

            // 4. Top 5 IPs
            System.out.println("\n=== 4. Top 5 des adresses IP ===");
            logs.mapToPair(log -> new Tuple2<>(log.ip, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap) // Swap to sort by count
                .sortByKey(false) // Descending
                .take(5)
                .forEach(t -> System.out.println(t._2 + " : " + t._1)); // Count : IP

            // 5. Top 5 Resources
            System.out.println("\n=== 5. Top 5 des ressources les plus demandées ===");
            logs.mapToPair(log -> new Tuple2<>(log.resource, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(5)
                .forEach(t -> System.out.println(t._2 + " : " + t._1));

            // 6. Distribution of HTTP codes
            System.out.println("\n=== 6. Répartition des requêtes par code HTTP ===");
             logs.mapToPair(log -> new Tuple2<>(log.code, 1))
                .reduceByKey(Integer::sum)
                .sortByKey() // Sort by code
                .collect()
                .forEach(t -> System.out.println("Code " + t._1 + " : " + t._2 + " requêtes"));
        }
    }

    private static LogEntry parseLine(String line) {
        Matcher m = PATTERN.matcher(line);
        if (m.find()) {
            return new LogEntry(
                m.group(1), // IP
                m.group(2), // Date
                m.group(3), // Method
                m.group(4), // Resource
                Integer.parseInt(m.group(5)), // Code
                Long.parseLong(m.group(6)) // Size
            );
        }
        return null; // Skip invalid lines
    }

    static class LogEntry implements java.io.Serializable {
        String ip;
        String date;
        String method;
        String resource;
        int code;
        long size;

        public LogEntry(String ip, String date, String method, String resource, int code, long size) {
            this.ip = ip;
            this.date = date;
            this.method = method;
            this.resource = resource;
            this.code = code;
            this.size = size;
        }
    }
}
