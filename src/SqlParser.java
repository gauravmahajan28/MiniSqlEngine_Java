import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public interface SqlParser 
{
	
	TGSqlParser getSqlParser();
	
	void attachQueryToParser(TGSqlParser parser);
	
	TSelectSqlStatement getParsedStatement(TGSqlParser parser);
	
	int getTableCount(TSelectSqlStatement statement);
	
	int getColumnCount(TSelectSqlStatement statement);
	
	
	
	
	

}
