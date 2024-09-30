import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private IntWritable year = new IntWritable();
    private DoubleWritable econsum = new DoubleWritable();
	private Text month = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	double consum=0;
    	String emonth;
        String[] fields = value.toString().split(";");
        String[] months = {"", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

        int yearVal = Integer.parseInt(fields[0]);
        for(int i=1; i<fields.length; i++) {
           	//set KEYOUT
           	emonth = months[i];
            month.set(emonth);
            //set VALUEOUT
          	consum = Double.parseDouble(fields[i]);
           	econsum.set(consum);
           	//write to collection
            context.write(month, econsum);
        }
    }
}
