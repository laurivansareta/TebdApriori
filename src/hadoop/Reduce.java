package hadoop;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
    	int totalTrans = Integer.parseInt(context.getConfiguration().get("linhasInicial"));
    	double count = 0, supMin = Float.parseFloat(context.getConfiguration().get("supMin"));
    	String outLinhas = "";
    	
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

 
 