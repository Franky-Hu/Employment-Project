import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(",", -1);
        if(line[17].length() == 6){return;}

        StringBuilder sb = new StringBuilder();
        sb.append(line[1]);
        sb.append(",");
        sb.append(line[4]);
        sb.append(",");
        sb.append(line[17]);


        // Write out the cleaned dataset
        context.write(new Text(sb.toString()), new IntWritable(1));

    }
}
