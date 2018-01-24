import java.util.ArrayList;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public interface SqlParser 
{
	
	TGSqlParser getSqlParser();
	
	void attachQueryToParser(TGSqlParser parser, String query);
	
	TSelectSqlStatement getParsedStatement(TGSqlParser parser);
	
	int getTableCount(TSelectSqlStatement statement);
	
	int getColumnCount(TSelectSqlStatement statement);
	
	ArrayList<String> getFromTableNames(TSelectSqlStatement statement);
	
	ArrayList<String> getSelectColumnNames(TSelectSqlStatement statement);
	
	
	
	
	

}
