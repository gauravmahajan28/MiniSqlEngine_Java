import java.util.ArrayList;
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

	public void readMetadata(String metadataFilePath) {
		// TODO Auto-generated method stub
		
	}

	public void initMapVariables() {
		// TODO Auto-generated method stub
		
	}
	

}
