

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import sun.net.www.MeteredStream;
import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class QueryEngine {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// args[0] contains sql query string
		
		try
		{
			System.out.println(args[0]);
			
			MetadataReaderImplementation metadataReaderImplementation = new MetadataReaderImplementation();
			metadataReaderImplementation.initMapVariables();
			metadataReaderImplementation.readMetadata("resources/metadata.txt");
			Map<String, ArrayList<String>> tableNameToColumns = metadataReaderImplementation.getTableNameToColumns();
			SqlParserImplementation sqlParserImplementation = new SqlParserImplementation();
			TGSqlParser sqlParser = sqlParserImplementation.getSqlParser();
			sqlParserImplementation.attachQueryToParser(sqlParser, args[0]);
			TSelectSqlStatement statement = sqlParserImplementation.getParsedStatement(sqlParser);
			
			
			ArrayList<String> fromTableNames = sqlParserImplementation.getFromTableNames(statement);
			
			if(metadataReaderImplementation.checkTableExists(fromTableNames) == false)
			{
				throw new Exception("tables do not exist");
			}
			
			ArrayList<String> selectColumnsNames = sqlParserImplementation.getSelectColumnNames(statement);
			
			
			
			System.out.println(sqlParserImplementation.getTableCount(statement));
			System.out.println(sqlParserImplementation.getColumnCount(statement));
			
		}
		catch(Exception e)
		{
			System.out.println("could not execute sql query :" + e.getMessage());
		}
		//	metadataReaderImplementation.readMetadata(metadataFilePath);
		
		/*
			TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvmysql);
			sqlParser.sqltext = "select p,q,r from A,B";
			if(sqlParser.parse() == 0)
				System.out.println("parsed");
			
			TSelectSqlStatement statement = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
			
			int tableCount = statement.joins.size();
			System.out.println("table count" + tableCount);
			
			
			
			int columnCount = statement.getResultColumnList().size(), pos = -1;
			
			System.out.println("column count " + columnCount);
	*/
		
		

	}

}
