package hadoop;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ReduceDois extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
		double totalTrans = Float.parseFloat(context.getConfiguration().get("linhasInicial"));
    	double supMin = Float.parseFloat(context.getConfiguration().get("supMin"));
    	String [] linha = null;
		String outLinhas = "";
    	for (Text val : values) {
    		outLinhas = val.toString();
    		break;
    	}    	
    	linha = outLinhas.split(",");
    	if ((linha.length /totalTrans) > supMin){
    		context.write(key, new Text(outLinhas));
    	}
    	    	
     }
	  
}

 
 