package hadoop;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ReduceDois extends Reducer<Text, Text, Text, Text> {
	double totalTrans = 5;
    double supMin = 0.5; 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {    	
    	String [] linha = values.toString().split(",");
		String outLinhas = "";
    	double count = 0; 
    	for (Text val : values) {
    		outLinhas = outLinhas.concat(val.toString()+",");
    		count++;
    	}    	
    	//System.out.println(outLinhas); //APAGAR
    	if ((count/totalTrans) > supMin){
    		context.write(new Text(key), new Text(outLinhas));
    	}
    	    	
     }
	  
}

 
 