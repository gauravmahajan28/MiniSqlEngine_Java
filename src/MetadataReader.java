import java.util.ArrayList;


public interface MetadataReader 
{
	
	void readMetadata(String metadataFilePath) throws Exception;
	
	void initMapVariables();
	
	boolean checkTableExists(ArrayList<String> tableNames);

}
