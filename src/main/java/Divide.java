import java.io.IOException;


import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class Divide {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,ArrayWritable> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text tabSeparatedData, Context context) throws IOException,  InterruptedException {
            LongWritable[] occurrences=new LongWritable[2];
            String[] splitArray = tabSeparatedData.toString().split("\t");
            System.out.println(splitArray[0]);
            //todo:use regex to get rid of spaces...
            Text currGram=new Text(splitArray[0]);
            long currOc=Long.parseLong(splitArray[2]);
            if(rowNumber.get()%2==1){
                occurrences[0].set(0);
                occurrences[1].set(currOc);
            }else {
                occurrences[0].set(currOc);
                occurrences[1].set(0);
            }
            context.write(currGram,new ArrayWritable(LongWritable.class,occurrences));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    /*
    public static class CombinerClass extends Reducer<Text,ArrayWritable,Text,ArrayWritable> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text threeGram, Iterable<ArrayWritable> occurencesArrays, Context context) throws IOException,  InterruptedException {
            long even=0;
            long odd=0;
            LongWritable[] occurrences=new LongWritable[2];
            for(ArrayWritable arr:occurencesArrays){
                LongWritable[] currOc=(LongWritable[]) arr.get();
                even+=currOc[0].get();
                odd+=currOc[1].get();
            }
            occurrences[0].set(even);
            occurrences[1].set(odd);
            context.write(threeGram,new ArrayWritable(LongWritable.class,occurrences));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    */

    public static class ReducerClass extends Reducer<Text,ArrayWritable,Text,ArrayWritable> {
        private Counter N;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            N = context.getCounter(Main.Counter.N_SUM);
        }

        @Override
        public void reduce(Text threeGram, Iterable<ArrayWritable> occurencesArrays, Context context) throws IOException,  InterruptedException {
            long even=0;
            long odd=0;
            LongWritable[] occurrences=new LongWritable[2];
            for(ArrayWritable arr:occurencesArrays){
                LongWritable[] currOc=(LongWritable[]) arr.get();
                even+=currOc[0].get();
                odd+=currOc[1].get();
            }
            occurrences[0].set(even);
            occurrences[1].set(odd);

            context.write(threeGram,new ArrayWritable(LongWritable.class,occurrences));

            N.increment(even+odd);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }




    public static class PartitionerClass extends Partitioner<Text,IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"DivideJob");
        job.setJarByClass(Divide.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}