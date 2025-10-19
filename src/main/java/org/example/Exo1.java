package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exo1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("VentesAnalysis").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.setLogLevel("WARN");

            JavaRDD<String> lines = sc.textFile("ventes.txt");

            // 1. Total sales by city
            System.out.println("=== Total des ventes par ville ===");
            JavaPairRDD<String, Double> salesByCity = lines.mapToPair(line -> {
                String[] parts = line.split(" ");
                String city = parts[1];
                Double price = Double.parseDouble(parts[3]);
                return new Tuple2<>(city, price);
            }).reduceByKey(Double::sum);

            salesByCity.collect().forEach(tuple ->
                    System.out.println(tuple._1() + " : " + tuple._2()));

            // 2. Total sales by product, city, and year
            System.out.println("\n=== Total des ventes par année, ville et produit ===");
            JavaPairRDD<String, Double> salesByYearCityProduct = lines.mapToPair(line -> {
                String[] parts = line.split(" ");
                String date = parts[0];
                String year = date.split("-")[0];
                String city = parts[1];
                String product = parts[2];
                Double price = Double.parseDouble(parts[3]);
                // Key format: Year - City - Product
                String key = year + " - " + city + " - " + product;
                return new Tuple2<>(key, price);
            }).reduceByKey(Double::sum);

            salesByYearCityProduct.collect().forEach(tuple ->
                    System.out.println(tuple._1() + " : " + tuple._2()));
        }
    }
}
