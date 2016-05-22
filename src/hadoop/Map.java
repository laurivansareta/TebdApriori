package hadoop;
import java.io.IOException;

 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class Map extends Mapper<LongWritable, Text, Text, Text> {
	int indiceLinha = 0;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       

        String linha = value.toString();
        String[] linhaSplit = linha.split(" ");
        indiceLinha++;
        for(int i=0; i < linhaSplit.length; i++){        
        	context.write(new Text(linhaSplit[i]+"/"), new Text(Integer.toString(indiceLinha)));        
        }        
    }
}