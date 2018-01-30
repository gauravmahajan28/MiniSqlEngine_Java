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
		
		File csvFile = new File("src/"+tableName + ".csv");
		
		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		
		String line = null;
		while((line = br.readLine())!= null)
		{
			ArrayList<String> row = new ArrayList<>();
			StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
			while(stringTokenizer.hasMoreTokens())
			{
				String token = stringTokenizer.nextToken();
				if(token.contains("\""))
				{
					token = token.substring(1, token.length()-1);
				}
				row.add(token);
			}
			tableData.add(row);
		}
		
		return tableData;
	}

}
