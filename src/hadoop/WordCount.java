package hadoop;

import org.apache.hadoop.fs.Path;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    //matriz
	/*  A     B
	 * 2 3   7 8
	 * 4 5   9 10
	 */

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File( "/home/kranz12/Documents/Projetos/trabalho_dois/out"));
		
		Configuration conf = new Configuration();
        // A is an m-by-n matrix; B is an n-by-p matrix.
        //conf.set("m", "2");
        //conf.set("n", "5");
       
        Job job = new Job(conf, "AprioriOneStep");
        job.setJarByClass(WordCount.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path("/home/kranz12/Documents/Projetos/trabalho_dois/T40I10D100K.dat"));
        FileOutputFormat.setOutputPath(job, new Path("/home/kranz12/Documents/Projetos/trabalho_dois/out"));
 
        job.waitForCompletion(true);
        
        //Segudo job
//        conf.set("caminhoSaida", "2");
        Job jobDois = new Job(conf, "AprioriJobDois");
        jobDois.setJarByClass(WordCount.class);
        
        jobDois.setOutputKeyClass(Text.class);
        jobDois.setOutputValueClass(Text.class);
 
        jobDois.setMapperClass(MapDois.class);
        jobDois.setReducerClass(ReduceDois.class);
 
        jobDois.setInputFormatClass(TextInputFormat.class);
        jobDois.setOutputFormatClass(TextOutputFormat.class);
       
        FileInputFormat.addInputPath(jobDois, new Path("/home/kranz12/Documents/Projetos/trabalho_dois/out/part-r-00000"));
        FileOutputFormat.setOutputPath(jobDois, new Path("/home/kranz12/Documents/Projetos/trabalho_dois/out/jobDois"));
 
        jobDois.waitForCompletion(true);
        
    }
}