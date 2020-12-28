import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;


public class joinResults {

    public static class MapperClass1 extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text tabSeparatedData, Context context) throws IOException,  InterruptedException {
            String[] data = tabSeparatedData.toString().split("\t");
            long r = Long.parseLong(data[1])+Long.parseLong(data[2]);
            Text rKey = new Text(Long.toString(r) + ":2");
            Text threeGram = new Text(data[0]);
            context.write(rKey,threeGram);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class MapperClass2 extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text tabSeparatedData, Context context) throws IOException,  InterruptedException {
            String[] data = tabSeparatedData.toString().split("\t");
            Text rKey = new Text(data[0]+":1");
            Text prob = new Text(data[1]);
            context.write(rKey,prob);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        String currentR;
        String prob;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            prob = "0";
        }

        @Override
        public void reduce(Text rKey, Iterable<Text> data , Context context) throws IOException,  InterruptedException {
            String[] key = rKey.toString().split(":");
            String r = key[0];
            String type = key[1];
            if(currentR == null && type.equals("1")){
                List<Text> dataList = Lists.newArrayList(data);
                prob = dataList.get(0).toString();
                currentR = r;
            }
            if(r.equals(currentR) && type.equals("2")){
                for (Text threeGram : data){
                    context.write(threeGram,new Text(prob));
                }
                currentR = null;
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }


    private static class PartitionerClass extends Partitioner<Text,Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions){
            return key.toString().split(":")[0].hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"joinResultsJob");
        job.setJarByClass(joinResults.class);
//        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
//        job.setCombinerClass(CombinerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperClass1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperClass2.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(PartitionerClass.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}