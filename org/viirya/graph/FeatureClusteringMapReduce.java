
package org.viirya.graph;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
/*
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
*/
import org.apache.hadoop.fs.*;
/*
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
*/
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;


public class FeatureClusteringMapReduce {

    private static boolean compression = false;

    private static int number_of_selected_features = 10;


    public static StringTokenizer tokenize(String line, String pattern) {
        StringTokenizer tokenizer = new StringTokenizer(line, pattern);
        return tokenizer;
    }

    public static StringTokenizer tokenize(Text value, String pattern) {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, pattern);
        return tokenizer;
    }
    
    public static class LoadClusterAndFeatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {
 
            StringTokenizer tokenizer = tokenize(value, " \t");

            if (tokenizer.countTokens() <= 3) {
                /* load feature */
                tokenizer = tokenize(value, " %");
                if (tokenizer.countTokens() < 2)
                    return;

                String image_features = tokenizer.nextToken();
                String image_id = tokenizer.nextToken();

                if (image_id != null && image_features != null)
                    context.write(new Text(image_id), new Text(image_features));

            } else if (tokenizer.countTokens() > 3) {
                /* load cluster data */
                String image_id = tokenizer.nextToken();
                String cluster_centroid = tokenizer.nextToken();

                if (image_id != null && cluster_centroid != null)
                    context.write(new Text(image_id), new Text(cluster_centroid));
            }

            return;
 
        }

    }
 
    public static class LoadClusterAndFeatureReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {

            String cluster_centroid = null;
            String features = null;
            //ArrayList features_in_cluster = new ArrayList(); 
            HashMap<String, Double> features_in_cluster = new HashMap<String, Double>();

            while (values.hasNext()) {
                Text value = values.next();

                StringTokenizer tokenizer = tokenize(value, ",:");

                if (tokenizer.countTokens() > 1) {
                    /* image feature */
                    features = value.toString();
                } else {
                   /* cluster */
                    cluster_centroid = value.toString();
                }

            }

            if (cluster_centroid == null)
                return;

            context.write(new Text(cluster_centroid), new Text(features));

        }
    }
 

    public static class AggregatorMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {
 
            StringTokenizer tokenizer = tokenize(value, "\t");

            String cluster_centroid = tokenizer.nextToken();
            String features = tokenizer.nextToken();

            context.write(new Text(cluster_centroid), new Text(features));

        }

    }

    public static class AggregatorReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Double> features_in_cluster = new HashMap<String, Double>();

            while (values.hasNext()) {
                Text value = values.next();

                StringTokenizer tokenizer = tokenize(value, ",:");

                if (tokenizer.countTokens() > 1) {
                    /* image feature */
                    while(tokenizer.hasMoreTokens()) {    
                        String feature_id = tokenizer.nextToken();
                        double feature_value = Double.parseDouble(tokenizer.nextToken());

                        if (features_in_cluster.containsKey(feature_id)) {
                            double old_feature_value = ((Double) features_in_cluster.get(feature_id)).doubleValue();
                            features_in_cluster.put(feature_id, new Double(old_feature_value + feature_value));
                        }
                        else {
                            features_in_cluster.put(feature_id, new Double(feature_value));
                        }
                            
                    }
                } 
            }

            // going to sort the feature list

            List<Map.Entry<String, Double>> list = new Vector<Map.Entry<String, Double>>(features_in_cluster.entrySet());
            java.util.Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> entry, Map.Entry<String, Double> entry1) {
                    // Return 0 for a match, -1 for less than and +1 for more then
                    return (entry.getValue().equals(entry1.getValue()) ? 0 : (entry.getValue() > entry1.getValue() ? -1 : 1));
                }
            });

            features_in_cluster.clear();

            StringBuffer strbuf = new StringBuffer();
            //for (Map.Entry<String, Double> entry : features_in_cluster.entrySet()) {
            int feature_counter = number_of_selected_features;
            Iterator itr = list.iterator();
            while(itr.hasNext() && feature_counter-- > 0) {
                Map.Entry<String, Double> entry = (Map.Entry<String, Double>)itr.next();
                String value = entry.getKey();
                strbuf.append(value + " ");
            }

            context.write(key, new Text(strbuf.toString()));

        }
    }

        
    private static void setJobConfCompressed(Configuration conf) {
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
    }


    public static void main(String[] args) throws Exception {

        String cluster_path = null;
        String feature_path = null;

        if (args.length < 2) {
            System.out.println("Usage: FeatureClusteringMapReduce <clusters path> <features path> [compress]");    
            System.exit(0);
        }

        cluster_path = args[0];
        feature_path = args[1];

        if (args.length == 3 && args[2].equals("compress"))
            compression = true;

        loadClusterAndFeature(cluster_path, feature_path);
        aggregatingClusterFeature();

    }
 
    public static void loadClusterAndFeature(String cluster_path, String feature_path) throws Exception {

        //JobConf job_loaddata = new JobConf(new Configuration(), FeatureClustering.class);

        Configuration conf = new Configuration();
        conf.setLong("dfs.block.size",134217728);
        if (compression)
            setJobConfCompressed(conf);

        Job job_loaddata = new Job(conf);
        job_loaddata.setJarByClass(FeatureClusteringMapReduce.class);
        job_loaddata.setJobName("LoadClusterAndFeature");

        FileInputFormat.setInputPaths(job_loaddata, new Path(cluster_path + "/*.gz"), new Path(feature_path + "/*"));
        FileOutputFormat.setOutputPath(job_loaddata, new Path("output/feature_clu_data/loadedfeatures"));

        job_loaddata.setOutputKeyClass(Text.class);
        job_loaddata.setOutputValueClass(Text.class);
        job_loaddata.setMapOutputKeyClass(Text.class);
        job_loaddata.setMapOutputValueClass(Text.class);
        job_loaddata.setMapperClass(LoadClusterAndFeatureMapper.class);
        job_loaddata.setReducerClass(LoadClusterAndFeatureReducer.class);
        //job_loaddata.setNumMapTasks(38);
        job_loaddata.setNumReduceTasks(19);
        //job_loaddata.setLong("dfs.block.size",134217728);


        try {
            job_loaddata.waitForCompletion(true);
        } catch(Exception e){
            e.printStackTrace();
        }

    }

    public static void aggregatingClusterFeature() throws Exception {

        //JobConf job_aggregator = new JobConf(new Configuration(), FeatureClustering.class);

        Configuration conf = new Configuration();
        conf.setLong("dfs.block.size",134217728);
        if (compression)
            setJobConfCompressed(conf);

        Job job_aggregator = new Job(conf);
        job_aggregator.setJarByClass(FeatureClusteringMapReduce.class);
        job_aggregator.setJobName("AggregatingClusterFeatures");

        FileInputFormat.setInputPaths(job_aggregator, new Path("output/feature_clu_data/loadedfeatures"));
        FileOutputFormat.setOutputPath(job_aggregator, new Path("output/feature_clu_data/output"));

        job_aggregator.setOutputKeyClass(Text.class);
        job_aggregator.setOutputValueClass(Text.class);
        job_aggregator.setMapOutputKeyClass(Text.class);
        job_aggregator.setMapOutputValueClass(Text.class);
        job_aggregator.setMapperClass(AggregatorMapper.class);
        job_aggregator.setReducerClass(AggregatorReducer.class);
        //job_aggregator.setNumMapTasks(38);
        job_aggregator.setNumReduceTasks(19);
        //job_aggregator.setLong("dfs.block.size",134217728);

        try {
            job_aggregator.waitForCompletion(true);        
        } catch(Exception e){
            e.printStackTrace();
        }

    } 
}


