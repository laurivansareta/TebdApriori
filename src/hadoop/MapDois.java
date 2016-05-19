package hadoop;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class MapDois extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       
    	String linha = "", chave ="", chaveArq = "", valoresSaida="", chavesSaida="";
		String[] valores = null, valoresArq = null, chaveValor = null, chaveValorArq = null;
		
    	FileReader arquivo = new FileReader(new File("/home/kranz12/Documents/Projetos/trabalho_dois/out/part-r-00000"));
    	BufferedReader lerArq = new BufferedReader(arquivo);
    	
    	//posições no array 0-Chave 1-Valor
    	chaveValor = value.toString().split("	");
    	chave = chaveValor[0];
    	valores = chaveValor[1].split(",");
    	
    	for (String linhaArq = lerArq.readLine(); linhaArq != null; linhaArq = lerArq.readLine(), valoresSaida="") {
    		chaveValorArq = linhaArq.split("	");
    		chaveArq = chaveValorArq[0];
//    		valoresArq = chaveValorArq[1].split(","); //ver se precisa apagar
    		if (!chave.equals(chaveArq)){
    			if (chave.compareTo(chaveArq) < 0 ){
    				chavesSaida = chave + chaveArq;
    			}else if (chave.compareTo(chaveArq) > 0 ){
    				chavesSaida = chaveArq + chave;
    			}
    			for (int i=0; i < valores.length; i++){
//    				System.out.println("valores: " + valores[i] + " chaveValorArq[1]: " + chaveValorArq[1]);
					if (chaveValorArq[1].contains(valores[i])){
						valoresSaida += valores[i];
						if (i < valores.length){
							valoresSaida += ",";
						}							
					}
    			}
    		}
//    		System.out.println(chaveValorArq[1]);
    		if (valoresSaida.length()>0){
	    		System.out.println("chave: " + chavesSaida + " valor: " + valoresSaida);
	    		context.write(new Text(chavesSaida), new Text(valoresSaida));
    		}
		}
    	
    	lerArq.close();
    }
}