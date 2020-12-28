import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class Sort {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text tabSeparatedData, Context context) throws IOException,  InterruptedException {
            String[] data=tabSeparatedData.toString().split("\t");
            System.out.println("data splitted"+ Arrays.toString(data));
            String[] threeGram=data[0].split(" ");
            System.out.println("one word "+ threeGram[0]+ " sec word "+threeGram[1]);
            String firstTwoAsc = threeGram[0]+" "+threeGram[1];
            System.out.println("prob to double"+Double.valueOf(data[1]));
            double compProb = 1-Double.valueOf(data[1]);
            context.write(new Text(firstTwoAsc+compProb),tabSeparatedData);
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
        public void reduce(Text twoGramAndCompProb, Iterable<Text> threeGramsAndProb , Context context) throws IOException,  InterruptedException {

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text twoGramAndCompProb, Iterable<Text> threeGramsAndProb, Context context) throws IOException,  InterruptedException {
            for(Text currGramAndProb: threeGramsAndProb){
                String[] output=currGramAndProb.toString().split("/t");
                context.write(new Text(output[0]),new Text(output[1]));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort.MapperClass.class);
        job.setReducerClass(Sort.ReducerClass.class);
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
