package hadoop;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ReduceDois extends Reducer<Text, Text, Text, Text> {
	double totalTrans = 697;
    double supMin = 0.01; 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {    	
    	String [] linha = null;
		String outLinhas = "";
    	
    	for (Text val : values) {
    		outLinhas = val.toString();
    		break;
    	}    	
    	linha = outLinhas.split(",");
//    	System.out.println(linha.length + "LauriGay");
//    	System.out.println("reduce2: " + key + " e " + outLinhas ); //APAGAR
    	if ((linha.length /totalTrans) > supMin){
    		context.write(key, new Text(outLinhas));
    	}
    	    	
     }
	  
}

 
 