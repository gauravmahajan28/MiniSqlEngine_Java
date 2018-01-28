import java.util.ArrayList;
import java.util.Map;


public interface QueryExecutionInterface 
{
	void handleNoWhereClause(ArrayList<String> fromTableNames, ArrayList<String> selectColumnsNames, Map<String, ArrayList<ArrayList<String>>> tableData, Map<String, ArrayList<String>> tableNameToColumns, int isDistinctFound) throws Exception;

	void handleWhereClause(ArrayList<String> fromTableNames, ArrayList<String> selectColumnsNames, Map<String, ArrayList<ArrayList<String>>> tableData, Map<String, ArrayList<String>> tableNameToColumns, int isDistinctFound, String whereClause) throws Exception;



}
