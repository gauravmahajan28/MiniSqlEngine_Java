import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MetadataReaderImplementation implements MetadataReader
{

	Map<String, ArrayList<String>> tableNameToColumns;
	
	/**
	 * @return the tableNameToColumns
	 */
	public Map<String, ArrayList<String>> getTableNameToColumns() {
		return tableNameToColumns;
	}

	/**
	 * @param tableNameToColumns the tableNameToColumns to set
	 */
	public void setTableNameToColumns(
			Map<String, ArrayList<String>> tableNameToColumns) {
		this.tableNameToColumns = tableNameToColumns;
	}

	public void readMetadata(String metadataFilePath) throws Exception{
		// TODO Auto-generated method stub
		File f = new File(metadataFilePath);
		BufferedReader br = new BufferedReader(new FileReader(f));
		
		String line = "";
		String currentTableName = "";
		while((line = br.readLine())!= null)
		{
			
			if(line.contains("<begin_table>") || line.contains("<end_table>"))
			{
				continue;
			}
			if(line.contains("table"))
			{
				currentTableName = line;
				tableNameToColumns.put(currentTableName, new ArrayList<String>());
				continue;
			}
			
			tableNameToColumns.get(currentTableName).add(line);
			
		}
		
		
	}

	public void initMapVariables() {
		// TODO Auto-generated method stub
		tableNameToColumns = new HashMap<>();
		
	}

	@Override
	public boolean checkTableExists(ArrayList<String> tableNames) {
		// TODO Auto-generated method stub
		for(int count = 0; count < tableNames.size(); count++)
		{
			if(tableNameToColumns.containsKey(tableNames.get(count)))
				continue;
			else
				return false;
		}
		return true;
		
	}
	

}
