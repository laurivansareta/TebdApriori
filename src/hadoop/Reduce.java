package hadoop;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class Reduce extends Reducer<Text, Text, Text, Text> {
	double totalTrans = 697; //colocar constante é o total de transações
	double supMin = 0.01; 
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {    	
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

 
 