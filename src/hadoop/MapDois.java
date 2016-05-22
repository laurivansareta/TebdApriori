package hadoop;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class MapDois extends Mapper<LongWritable, Text, Text, Text> {
	
	public String ordenaChaves(String [] chaves){
		String chaveRetorno = "";
		Arrays.sort(chaves);
		
		for(int i = 0; i < chaves.length; i++)
			chaveRetorno += chaves[i] + "/";
		
		return chaveRetorno;
	}
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       
    	String chave ="", chaveArq = "", valoresSaida="", chavesSaida="";
		String[] valores = null, chaveValorArq = null, chavesDoArquivo = null;
		String[] chaveValor = value.toString().split("	"), tamanhoKey = key.toString().split("/");
			
    	FileReader arquivo = new FileReader(new File("/home/kranz12/Documents/Projetos/trabalho_dois/out/job"+(tamanhoKey.length)+"/part-r-00000"));
    	BufferedReader lerArq = new BufferedReader(arquivo);
    	
    	//posições no array 0-Chave 1-Valor
    	chave = chaveValor[0];
    	valores = chaveValor[1].split(","); 
    	
    	for (String linhaArq = lerArq.readLine(); linhaArq != null; linhaArq = lerArq.readLine(),chavesSaida="",valoresSaida="") {
    		chaveValorArq = linhaArq.split("	"); //linha completa do arquivo
    		chaveArq = chaveValorArq[0]; // chave do arquivo
    		if (!chave.equals(chaveArq)){ // verificando se a chave é igual a chave do arquivo
    			chavesDoArquivo = chaveArq.split("/"); // vetor de chaves da linha do arquivo
    			for(int cont = 0;cont < chavesDoArquivo.length; cont++){
    				chavesSaida = chave;
    				if (!chave.contains(chavesDoArquivo[cont])){
						chavesSaida += chavesDoArquivo[cont] + "/";		
						chavesSaida = ordenaChaves(chavesSaida.split("/")); //FUNÇÃO DE ORDENAÇÃO DA CHAVE
		    			for (int i=0; i < valores.length; i++){
							if (chaveValorArq[1].contains(valores[i])){
								valoresSaida += valores[i];
								if (i < valores.length){
									valoresSaida += ",";
								}							
							}
		    			}
		        		if (valoresSaida.length()>0){
		    	    		context.write(new Text(chavesSaida), new Text(valoresSaida));
		        		}
    				}
    			}
    		}

		}
    	
    	lerArq.close();
    }
}