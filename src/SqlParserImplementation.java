import java.util.ArrayList;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;


public class SqlParserImplementation implements SqlParser{

	@Override
	public TGSqlParser getSqlParser() {
		// TODO Auto-generated method stub
		return new TGSqlParser(EDbVendor.dbvmysql);
	}

	@Override
	public void attachQueryToParser(TGSqlParser parser, String query) {
		// TODO Auto-generated method stub
		parser.sqltext = query;
	}

	@Override
	public TSelectSqlStatement getParsedStatement(TGSqlParser parser) {
		// TODO Auto-generated method stub
		if(parser.parse() == 0)
			return (TSelectSqlStatement) parser.sqlstatements.get(0);
		else
			return null;
	}

	@Override
	public int getTableCount(TSelectSqlStatement statement) {
		// TODO Auto-generated method stub
		return statement.joins.size();
	}

	@Override
	public int getColumnCount(TSelectSqlStatement statement) {
		// TODO Auto-generated method stub
		return  statement.getResultColumnList().size();
	}

	@Override
	public ArrayList<String> getFromTableNames(TSelectSqlStatement statement) {
		// TODO Auto-generated method stub
		int numberOfFromTables = statement.joins.size();
		ArrayList<String> fromTableNames = new ArrayList<String>();
		for (int count = 0; count < numberOfFromTables; count++) 
		{
			TJoin join = statement.joins.getJoin(count);
			String fromTableName = join.getTable().toString();
			fromTableNames.add(fromTableName);
		}
		return fromTableNames;
	}

	@Override
	public ArrayList<String> getSelectColumnNames(TSelectSqlStatement statement) {
		// TODO Auto-generated method stub
		
		int selectColumnsCount = statement.getResultColumnList().size();
		ArrayList<String> selectColumnNames = new ArrayList<String>();
		
		for (int count = 0; count < selectColumnsCount; count++) 
		{
			TResultColumn result = statement.getResultColumnList().getResultColumn(count);
			String fieldName = result.getExpr().toString();
			selectColumnNames.add(fieldName);
		}
		return selectColumnNames;
	}

}
