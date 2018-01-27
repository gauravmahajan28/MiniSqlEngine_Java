import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class FileOperationsImplementation implements FileOperations {

	@Override
	public ArrayList<ArrayList<String>> readFile(String tableName) throws Exception
	{
		ArrayList<ArrayList<String>> tableData = new ArrayList<>();
		
		File csvFile = new File("resources/" + tableName + ".csv");
		
		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		
		String line = null;
		while((line = br.readLine())!= null)
		{
			ArrayList<String> row = new ArrayList<>();
			StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
			while(stringTokenizer.hasMoreTokens())
			{
				row.add(stringTokenizer.nextToken());
			}
			tableData.add(row);
		}
		
		return tableData;
	}

}
