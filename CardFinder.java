import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class CardFinder{
  public static class CardMap extends Mapper<LongWritable, Text, Text, IntWritable>{
    //The mapper splits the input string as key and values
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
      String valueString = value.toString();
      String[] StringArr = valueString.split(" ");
      context.write(new Text(StringArr[0]), new IntWritable(Integer.parseInt(StringArr[1])));
    }
  }

  public static class CardReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    //Reducer determines the missing cards
    public void reduce(Text key, Iterable<IntWritable> value, Context context)throws IOException, InterruptedException{
      //Holds the values of cards given in a key
      ArrayList<Integer> givendeck = new ArrayList<Integer>();
      //Get the values of cards given in a key
      for (IntWritable val : value) {
        int cardnumber=val.get();
        givendeck.add(cardnumber);
      }
      //Loop whole deck to determine missing cards
      for (int i = 1;i <= 13;i++){
        //If card is not in givendeck, it should be output
        if(!givendeck.contains(i)){
          context.write(key, new IntWritable(i));
        }
      }
    }
  }
  //Main
  public static void main(String[] args)throws Exception{
    Configuration config = new Configuration();
    Job job = new Job(config, "CardFinder");
    job.setJarByClass(CardFinder.class);
    job.setMapperClass(CardMap.class);
    job.setReducerClass(CardReduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
