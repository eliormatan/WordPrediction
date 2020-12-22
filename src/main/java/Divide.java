import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Divide {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,ArrayWritable> {
        private static Pattern HEBREW_PATTERN = Pattern.compile("^[א-ת]+.*$|^\\d*\\s[א-ת]+.*$");
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text tabSeparatedData, Context context) throws IOException,  InterruptedException {
            LongWritable[] occurrences=new LongWritable[2];
            String[] splitArray = tabSeparatedData.toString().split("\t");
            String data = tabSeparatedData.toString();
            System.out.println("data to string :");
            System.out.println(data);
//            Matcher m = HEBREW_PATTERN.matcher(splitArray[0]);
//            if(m.matches()){
//                System.out.println("matches :");
//                System.out.println(m.group());
//            }
//
//            String regexHebrewPattern = "([\\p{InHebrew}]+)";
//            Pattern patternHebrew = Pattern.compile(regexHebrewPattern, Pattern.UNICODE_CASE);
//            Matcher hebrewMatcher = patternHebrew.matcher(splitArray[0]);
//            if(hebrewMatcher.matches()){
//                System.out.println("second matcher :");
//                System.out.println(hebrewMatcher.group());
//            }

//            //todo:use regex to get rid of spaces...
//            Text currGram=new Text(splitArray[0]);
//            long currOc=Long.parseLong(splitArray[2]);
//            if(rowNumber.get()%2==1){
//                occurrences[0].set(0);
//                occurrences[1].set(currOc);
//            }else {
//                occurrences[0].set(currOc);
//                occurrences[1].set(0);
//            }
//            context.write(currGram,new ArrayWritable(LongWritable.class,occurrences));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }


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
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}