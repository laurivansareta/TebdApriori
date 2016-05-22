package hadoop;

import org.apache.hadoop.fs.Path;

import java.io.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static int verificaNumeroLinhas(File arq) throws IOException{ 
		int total = 0;
		LineNumberReader lnr = new LineNumberReader(new FileReader(arq));
		lnr.skip(Long.MAX_VALUE);
		total = lnr.getLineNumber();
		lnr.close();
		
		return total;
	}
	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File( "/home/kranz12/Documents/Projetos/trabalho_dois/out"));
		Configuration conf = new Configuration();
		String DadosIniciais = "/home/kranz12/Documents/Projetos/trabalho_dois/letras.dat";
		
		File arq = new File(DadosIniciais);  
		conf.set("supMin", "0.5");
		conf.set("linhasInicial", ""+verificaNumeroLinhas(arq));
		
        Job job = new Job(conf, "AprioriOneStep");
        job.setJarByClass(WordCount.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(DadosIniciais));
        FileOutputFormat.setOutputPath(job, new Path("/home/kranz12/Documents/Projetos/trabalho_dois/out/job1"));
        
        job.waitForCompletion(true);
        
        arq = new File("/home/kranz12/Documents/Projetos/trabalho_dois/out/job1/part-r-00000");
        
        for(int jobs = 2; jobs <= verificaNumeroLinhas(arq); jobs++){
          Job jobDois = new Job(conf, "AprioriJobDois");
          jobDois.setJarByClass(WordCount.class);
          
          jobDois.setOutputKeyClass(Text.class);
          jobDois.setOutputValueClass(Text.class);
   
          jobDois.setMapperClass(MapDois.class);
          jobDois.setReducerClass(ReduceDois.class);
   
          jobDois.setInputFormatClass(TextInputFormat.class);
          jobDois.setOutputFormatClass(TextOutputFormat.class);
         
          FileInputFormat.addInputPath(jobDois, new Path("/home/kranz12/Documents/Projetos/trabalho_dois/out/job"+(jobs-1)+"/part-r-00000"));
          FileOutputFormat.setOutputPath(jobDois, new Path("/home/kranz12/Documents/Projetos/trabalho_dois/out/job"+jobs));
   
          jobDois.waitForCompletion(true);       
          
          arq = new File("/home/kranz12/Documents/Projetos/trabalho_dois/out/job"+jobs+"/part-r-00000");
          if (verificaNumeroLinhas(arq) == 0){
        	  FileUtils.deleteDirectory(new File( "/home/kranz12/Documents/Projetos/trabalho_dois/out/job"+jobs));
        	  break;
          }
        }        
    }
}