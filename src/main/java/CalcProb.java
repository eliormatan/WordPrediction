import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


public class CalcProb {

    public enum Counter{N_SUM};

    public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text tabSeparatedData, Context context) throws IOException,  InterruptedException {
            String[] data = tabSeparatedData.toString().split("\t");
            long r = Long.parseLong(data[1])+Long.parseLong(data[2]);
            String rKey = Long.toString(r);
            if(Long.parseLong(data[1])!=0){
                String r1Val = data[2]+"\t1";
                context.write(new Text(rKey),new Text(r1Val));
            }
            if(Long.parseLong(data[2])!=0){
                String r2Val = data[1]+"\t1";
                context.write(new Text(rKey),new Text(r2Val));
            }
            context.getCounter(Counter.N_SUM).increment(r);
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
        public void reduce(Text rKey, Iterable<Text> rValues , Context context) throws IOException,  InterruptedException {
            long Nr = 0;
            long Tr = 0;
            for (Text val : rValues){
                String[] values  = val.toString().split("\t");
                Tr += Long.parseLong(values[0]);
                Nr += Long.parseLong(values[1]);
            }
            String TrNr = Tr + "\t" + Nr;
            context.write(rKey,new Text(TrNr));

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        private long N;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            N = currentJob.getCounters().findCounter(Counter.N_SUM).getValue();
        }

        @Override
        public void reduce(Text rKey, Iterable<Text> rValues, Context context) throws IOException,  InterruptedException {
            double Nr = 0;
            double Tr = 0;
            for (Text val : rValues){
                String[] values  = val.toString().split("\t");
                Tr += Long.parseLong(values[0]);
                Nr += Long.parseLong(values[1]);
            }
            double p = 0;
            if(Nr != 0){
                p = Tr/((double)N *Nr);
            }
            context.write(rKey,new Text(Double.toString(p)));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"CalcProbJob");
        job.setJarByClass(CalcProb.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
//        job.setCombinerClass(CombinerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}