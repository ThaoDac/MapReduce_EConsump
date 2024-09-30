import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	private DoubleWritable avgEConsum = new DoubleWritable();
	private Text maxMonth = new Text();
	private double maxAvg = Double.NEGATIVE_INFINITY;

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int valueSum = 0;
		double avg = 0;
		
		// TODO Auto-generated method stub
        for (DoubleWritable val : values) {
//            context.write(key, val);
        	valueSum += val.get(); 
        }
        
        avg =(valueSum/5.0);
//        avgEConsum.set(valueSum/5.0);        
//        context.write(key, avgEConsum);
        
        if (avg > maxAvg) {
        	maxAvg = avg;
        	maxMonth.set(key); 
        }
	}

	@Override
	public void run(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
		avgEConsum.set(maxAvg);        
    	context.write(maxMonth, avgEConsum);
	}
}
