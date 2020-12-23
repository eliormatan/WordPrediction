import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Divide {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text tabSeparatedData, Context context) throws IOException,  InterruptedException {
            Text occurences = new Text();
            String[] splitArray = tabSeparatedData.toString().split("\t");
//            String data = tabSeparatedData.toString();
//            System.out.println("data to string :");
//            System.out.println(data);

            //todo:use regex to get rid of spaces...
            Text currGram=new Text(splitArray[0]);
            if(rowNumber.get()%2==1){
                occurences.set(String.format("%s\t%s","0",splitArray[2]));

            }else {
                occurences.set(String.format("%s\t%s",splitArray[2],"0"));
            }
            context.write(currGram,occurences);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class CombinerClass extends Reducer<Text,Text,Text,Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text nGram, Iterable<Text> occurences, Context context) throws IOException,  InterruptedException {
            long even=0L;
            long odd=0L;
            for(Text occ:occurences){
                String[] splitOcc = occ.toString().split("\t");
                even+=Long.parseLong(splitOcc[0]);
                odd+=Long.parseLong(splitOcc[1]);
            }
            Text occSum = new Text();
            occSum.set(String.format("%d\t%d",even, odd));

            context.write(nGram,occSum);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        private Counter N;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            N = context.getCounter(Main.Counter.N_SUM);
        }

        @Override
        public void reduce(Text nGram, Iterable<Text> occurences, Context context) throws IOException,  InterruptedException {
            long even=0L;
            long odd=0L;
            for(Text occ:occurences){
                String[] splitOcc = occ.toString().split("\t");
                even+=Long.parseLong(splitOcc[0]);
                odd+=Long.parseLong(splitOcc[1]);
            }
            Text occSum = new Text();
            occSum.set(String.format("%d\t%d",even, odd));

            context.write(nGram,occSum);

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
        job.setCombinerClass(CombinerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}