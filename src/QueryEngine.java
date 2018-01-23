

import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class QueryEngine {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		
		while(true)
		{
		
			TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvmysql);
			sqlParser.sqltext = "select p,q,r from A,B";
			if(sqlParser.parse() == 0)
				System.out.println("parsed");
			
			TSelectSqlStatement statement = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
			
			int tableCount = statement.joins.size();
			System.out.println("table count" + tableCount);
			
			
			
			int columnCount = statement.getResultColumnList().size(), pos = -1;
			
			System.out.println("column count " + columnCount);
			
		}
		
		

	}

}
