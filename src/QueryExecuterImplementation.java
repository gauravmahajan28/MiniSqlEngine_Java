import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.sun.jndi.url.corbaname.corbanameURLContextFactory;


public class QueryExecuterImplementation implements QueryExecutionInterface
{

	@Override
	public void handleNoWhereClause(ArrayList<String> fromTableNames,
			ArrayList<String> selectColumnsNames,
			Map<String, ArrayList<ArrayList<String>>> tableData, Map<String, ArrayList<String>> tableNameToColumns, int isDistinctFound) throws Exception
	{
		// TODO Auto-generated method stub
		
		
		
		ArrayList<ArrayList<String>> outputData = new ArrayList<>();
		
		
		if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).equals("*"))
		{
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
			outputData.add(headers);
			
			
			Map<ArrayList<String>, Boolean> mapForDistinct = new HashMap<>();
			
			// handles special case
			if(fromTableNames.size() > 2)
			{
				ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
					{
						ArrayList<String> row = new ArrayList<>();
						row.addAll(tableData.get(fromTableNames.get(0)).get(count));
						row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
						if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
						{
							tempOutputData.add(row);
							mapForDistinct.put(row, new Boolean(true));
						}
						else if(isDistinctFound == 0)
						{
							tempOutputData.add(row);
						}
							
					}
				} // for
				
				
				for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
				{
					
					ArrayList<ArrayList<String>> tempData = new ArrayList<>();
					
					
					for(int count = 0; count < tempOutputData.size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tempOutputData.get(count));
							row.addAll(tableData.get(fromTableNames.get(tableCount)).get(innerCount));
							if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
							{
								tempData.add(row);
								mapForDistinct.put(row, new Boolean(true));
							}
							else if(isDistinctFound == 0)
							{
								tempData.add(row);
							}
								
						}
					} // for
					
					tempOutputData.clear();
					tempOutputData.addAll(tempData);
					
				} // for
				
				outputData.addAll(tempOutputData);
				
				
			} //if
			else
			{
				// only one table
				if(fromTableNames.size() == 1)
				{
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						// distinct
						if(isDistinctFound == 1 && mapForDistinct.get(tableData.get(fromTableNames.get(0)).get(count)) == null)
						{
							outputData.add(tableData.get(fromTableNames.get(0)).get(count));
							mapForDistinct.put(tableData.get(fromTableNames.get(0)).get(count), new Boolean(true));
							
						}
						else if(isDistinctFound == 0)
						{
							outputData.add(tableData.get(fromTableNames.get(0)).get(count));
						}
						
					}
				} //if
				// two tables
				else
				{
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tableData.get(fromTableNames.get(0)).get(count));
							row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
							if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
							{
								outputData.add(row);
								mapForDistinct.put(row, new Boolean(true));
							}
							else if(isDistinctFound == 0)
							{
								outputData.add(row);
							}
								
						}
					} // for
					
				} // else
			}// else
			displayResults(outputData);
		} // if select *
		
		// max
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("max("))
		{
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
			
			
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add("max("+fromTableNames.get(0)+"." + columnName+")");
				outputData.add(header);
				
				int max = Integer.MIN_VALUE;
				
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					
					if(max < Integer.parseInt(tableData.get(fromTableNames.get(0)).get(count).get(index)))
					{
						max = Integer.parseInt(tableData.get(fromTableNames.get(0)).get(count).get(index));
					}
						
				}
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf(max));
				outputData.add(maxData);
				displayResults(outputData);
			}
			
		}
		//sum
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("sum("))
		{
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
			
			
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add("sum("+fromTableNames.get(0)+"." + columnName+")");
				outputData.add(header);
				
				int sum = 0;
				
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					
					sum += Integer.parseInt(tableData.get(fromTableNames.get(0)).get(count).get(index));
						
				}
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf(sum));
				outputData.add(maxData);
				displayResults(outputData);
			}
				
		}
		//avg
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("avg("))
		{
		
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
			
			
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			
			
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add("avg("+fromTableNames.get(0)+"." + columnName+")");
				outputData.add(header);
				
				int avg = 0;
				int rowCount = 0;
				
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					avg += Integer.parseInt(tableData.get(fromTableNames.get(0)).get(count).get(index));
					rowCount++;
						
				}
				float average = ((float)avg / rowCount);
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.format("%.2f", average));
				outputData.add(maxData);
				displayResults(outputData);
			}
			
			
		}
		// min
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("min("))
		{
			
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
			
			
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add("min("+fromTableNames.get(0)+"." + columnName+")");
				outputData.add(header);
				
				int min = Integer.MAX_VALUE;
				
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					
					if( Integer.parseInt(tableData.get(fromTableNames.get(0)).get(count).get(index)) < min)
					{
						min = Integer.parseInt(tableData.get(fromTableNames.get(0)).get(count).get(index));
					}
						
				}
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf(min));
				outputData.add(maxData);
				displayResults(outputData);
			}
			
			
		} // min
		
		/**
		 * handle from here
		 */
		else // select one or more columns
		{
			ArrayList<String> headers = new ArrayList<>();
			Map<String, ArrayList<Integer>> tableNameTocolumnIndicesRequired = new HashMap<>();
			// adding headers
			/*
			 * 
			 * only supported formats are select table.col, table2.col2  / sleect col1,col2
			 */
			
			
			
			// if select fields  does not contain tableName.columnName
			if(!selectColumnsNames.get(0).contains("."))
			{
			
				for(int count = 0; count < fromTableNames.size(); count++)
				{
					ArrayList<Integer> columnIndices = new ArrayList<>();
					
					for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
					{
						if(selectColumnsNames.contains(tableNameToColumns.get(fromTableNames.get(count)).get(name)))
						{
							columnIndices.add(name);
							headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));	
						}
					}
					tableNameTocolumnIndicesRequired.put(fromTableNames.get(count), columnIndices);
				}
			}
			else
			{
				for(int count = 0; count < fromTableNames.size(); count++)
				{
					ArrayList<Integer> columnIndices = new ArrayList<>();
					
					for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
					{
						if(selectColumnsNames.contains(fromTableNames.get(count)+"."+tableNameToColumns.get(fromTableNames.get(count)).get(name)))
						{
							columnIndices.add(name);
							headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));	
						}
					}
					tableNameTocolumnIndicesRequired.put(fromTableNames.get(count), columnIndices);
				}
			}
			
			
			if(headers.size() == 0 || headers.size() != selectColumnsNames.size())
			{
				throw new Exception("column not found");
			}
			
			outputData.add(headers);
			
			// handles special case
			if(fromTableNames.size() > 2)
			{
				ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					
					ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
					ArrayList<String> row = new ArrayList<>();
					for(int index = 0; index < indices.size(); index++)
					{
						row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index)));
					}
					
					for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
					{
						
						ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(1));
						ArrayList<String> innerRow = new ArrayList<>();
						for(int index = 0; index < innerIndices.size(); index++)
						{
							innerRow.add(tableData.get(fromTableNames.get(1)).get(innerCount).get(innerIndices.get(index)));
						}
						
						ArrayList<String> nestedRow = new ArrayList<>();
						nestedRow.addAll(row);
						nestedRow.addAll(innerRow);
						tempOutputData.add(nestedRow);								
					}
				} // for
				
				
				for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
				{
					
					ArrayList<ArrayList<String>> tempData = new ArrayList<>();
					
					
					for(int count = 0; count < tempOutputData.size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(tableCount));
							ArrayList<String> innerRow = new ArrayList<>();
							for(int index = 0; index < innerIndices.size(); index++)
							{
								innerRow.add(tableData.get(fromTableNames.get(tableCount)).get(innerCount).get(innerIndices.get(index)));
							}
							row.addAll(tempOutputData.get(count));
							row.addAll(innerRow);
							tempData.add(row);
							
								
						}
					} // for
					
					
					
					tempOutputData.clear();
					tempOutputData.addAll(tempData);
					
					
				} // for
				
				outputData.addAll(tempOutputData);
				
		
			} //if
			else
			{
				// only one table
				if(fromTableNames.size() == 1)
				{
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						
						ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
						ArrayList<String> row = new ArrayList<>();
						
						
						for(int index = 0; index < indices.size(); index++)
						{
							row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index)));
						}
						outputData.add(row);
					}
				} //if
				// two tables
				else
				{
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						
						ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
						ArrayList<String> row = new ArrayList<>();
						for(int index = 0; index < indices.size(); index++)
						{
							row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index)));
						}
						
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							
							ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(1));
							ArrayList<String> innerRow = new ArrayList<>();
							for(int index = 0; index < innerIndices.size(); index++)
							{
								innerRow.add(tableData.get(fromTableNames.get(1)).get(innerCount).get(innerIndices.get(index)));
							}
							
							ArrayList<String> nestedRow = new ArrayList<>();
							nestedRow.addAll(row);
							nestedRow.addAll(innerRow);
							outputData.add(nestedRow);								
						}
					} // for
					
				} // else
			}// else
			if(isDistinctFound == 1)
				displayDistinctResults(outputData);
			else 
				displayResults(outputData);
		}
		
		
	} // function end
	
	
	
	private void displayResults(ArrayList<ArrayList<String>> outputData) {
		// TODO Auto-generated method stub
		
		for(int count = 0; count < outputData.size(); count++)
		{
			for(int innerCount = 0; innerCount < outputData.get(count).size(); innerCount++)
			{
				System.out.print(outputData.get(count).get(innerCount));
				if(innerCount != outputData.get(count).size() - 1)
				{
					System.out.print(",");
				}
			}
			System.out.println();
		}
		
	}
	
	
	private void displayDistinctResults(ArrayList<ArrayList<String>> outputData) {
		// TODO Auto-generated method stub
		Map<ArrayList<String>, Boolean> isRowUnique = new HashMap<>();
		
		for(int count = 0; count < outputData.size(); count++)
		{
			if(isRowUnique.get(outputData.get(count)) == null)
			{
				isRowUnique.put(outputData.get(count), new Boolean(true));
			}
			else
			{
				outputData.remove(count);
				count--;
			}
		}
		
		
		for(int count = 0; count < outputData.size(); count++)
		{
			for(int innerCount = 0; innerCount < outputData.get(count).size(); innerCount++)
			{
				System.out.print(outputData.get(count).get(innerCount));
				if(innerCount != outputData.get(count).size() - 1)
				{
					System.out.print(",");
				}
			}
			System.out.println();
		}
		
	}
	
	

	public void handleSelectStar(ArrayList<String> fromTableNames,
			ArrayList<String> selectColumnsNames,
			Map<String, ArrayList<ArrayList<String>>> tableData) {
		// TODO Auto-generated method stub
		
		
		
	}



	@Override
	public void handleWhereClause(ArrayList<String> fromTableNames,
			ArrayList<String> selectColumnsNames,
			Map<String, ArrayList<ArrayList<String>>> tableData,
			Map<String, ArrayList<String>> tableNameToColumns,
			int isDistinctFound, String whereClause) throws Exception {
		
		

		ArrayList<ArrayList<String>> outputData = new ArrayList<>();
		
		// select * with where condition
		if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).equals("*"))
		{
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
			outputData.add(headers);
			
			
			Map<ArrayList<String>, Boolean> mapForDistinct = new HashMap<>();
			
			// handles special case
			if(fromTableNames.size() > 2)
			{
				ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
					{
						ArrayList<String> row = new ArrayList<>();
						row.addAll(tableData.get(fromTableNames.get(0)).get(count));
						row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
						if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
						{
							tempOutputData.add(row);
							mapForDistinct.put(row, new Boolean(true));
						}
						else if(isDistinctFound == 0)
						{
							tempOutputData.add(row);
						}
							
					}
				} // for
				
				
				for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
				{
					
					ArrayList<ArrayList<String>> tempData = new ArrayList<>();
					
					
					for(int count = 0; count < tempOutputData.size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tempOutputData.get(count));
							row.addAll(tableData.get(fromTableNames.get(tableCount)).get(innerCount));
							if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
							{
								tempData.add(row);
								mapForDistinct.put(row, new Boolean(true));
							}
							else if(isDistinctFound == 0)
							{
								tempData.add(row);
							}
								
						}
					} // for
					
					
					
					tempOutputData.clear();
					tempOutputData.addAll(tempData);
					
					
				} // for
				
				outputData.addAll(tempOutputData);
				
	
				
				
			} //if
			else
			{
				// only one table
				if(fromTableNames.size() == 1)
				{
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						// distinct
						if(isDistinctFound == 1 && mapForDistinct.get(tableData.get(fromTableNames.get(0)).get(count)) == null)
						{
							outputData.add(tableData.get(fromTableNames.get(0)).get(count));
							mapForDistinct.put(tableData.get(fromTableNames.get(0)).get(count), new Boolean(true));
							
						}
						else if(isDistinctFound == 0)
						{
							outputData.add(tableData.get(fromTableNames.get(0)).get(count));
						}
						
					}
				} //if
				// two tables
				else
				{
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tableData.get(fromTableNames.get(0)).get(count));
							row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
							if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
							{
								outputData.add(row);
								mapForDistinct.put(row, new Boolean(true));
							}
							else if(isDistinctFound == 0)
							{
								outputData.add(row);
							}
								
						}
					} // for
					
				} // else
			}// else
			outputData = filterWhereClause(outputData, whereClause);
			displayResults(outputData);
		} // if select *
		
		// max
		// select max(D) with where condition
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("max("))
		{
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
						
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			
			
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add(fromTableNames.get(0)+"." + columnName);
				outputData.add(header);
				
				Map<String, ArrayList<Integer>> tableNameTocolumnIndicesRequired = new HashMap<>();
				// adding headers
				for(int count = 0; count < fromTableNames.size(); count++)
				{
					ArrayList<Integer> columnIndices = new ArrayList<>();
					
					for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
					{
						if(header.contains(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name)))
						{
							columnIndices.add(name);
						//	headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
							
						}
					}
					tableNameTocolumnIndicesRequired.put(fromTableNames.get(count), columnIndices);
				}
				if(headers.size() == 0)
				{
					throw new Exception("column not found");
				}
				
				
				
				// handles special case
				if(fromTableNames.size() > 2)
				{
					ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tableData.get(fromTableNames.get(0)).get(count));
							row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
							
								tempOutputData.add(row);
							
								
						}
					} // for
					
					
					for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
					{
						
						ArrayList<ArrayList<String>> tempData = new ArrayList<>();
						
						
						for(int count = 0; count < tempOutputData.size(); count++)
						{
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
							{
								ArrayList<String> row = new ArrayList<>();
								row.addAll(tempOutputData.get(count));
								row.addAll(tableData.get(fromTableNames.get(tableCount)).get(innerCount));
								
									tempData.add(row);
								
									
							}
						} // for
						
					
						tempOutputData.clear();
						tempOutputData.addAll(tempData);
						
						
					} // for
					
					outputData.addAll(tempOutputData);
					
					
					
					
				} //if
				else
				{
					// only one table
					if(fromTableNames.size() == 1)
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							
							
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							outputData.add(row);
						}
					} //if
					// two tables
					else
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
							{
								
								ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(1));
								ArrayList<String> innerRow = new ArrayList<>();
								for(int index2 = 0; index2 < innerIndices.size(); index2++)
								{
									innerRow.add(tableData.get(fromTableNames.get(1)).get(innerCount).get(innerIndices.get(index2)));
								}
								
								ArrayList<String> nestedRow = new ArrayList<>();
								nestedRow.addAll(row);
								nestedRow.addAll(innerRow);
								outputData.add(nestedRow);								
							}
						} // for
						
					} // else
				}// else
				
				outputData = filterWhereClause(outputData, whereClause);
				
				int max = Integer.MIN_VALUE;
				
				for(int count = 1; count < outputData.size(); count++)
				{
					if(Integer.parseInt(outputData.get(count).get(0)) > max)
						max = Integer.parseInt(outputData.get(count).get(0));
					outputData.remove(count);
					count--;
				}
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf(max));
				outputData.add(maxData);
				header.set(0, "max("+header.get(0)+")");
				outputData.set(0, header);
				displayResults(outputData);
			}
			
		}
		//avg
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("avg("))
		{
		
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
						
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			
			
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add(fromTableNames.get(0)+"." + columnName);
				outputData.add(header);
				
				Map<String, ArrayList<Integer>> tableNameTocolumnIndicesRequired = new HashMap<>();
				// adding headers
				for(int count = 0; count < fromTableNames.size(); count++)
				{
					ArrayList<Integer> columnIndices = new ArrayList<>();
					
					for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
					{
						if(header.contains(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name)))
						{
							columnIndices.add(name);
						//	headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
							
						}
					}
					tableNameTocolumnIndicesRequired.put(fromTableNames.get(count), columnIndices);
				}
				if(headers.size() == 0)
				{
					throw new Exception("column not found");
				}
				
				
				
				// handles special case
				if(fromTableNames.size() > 2)
				{
					
					ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tableData.get(fromTableNames.get(0)).get(count));
							row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
							
								tempOutputData.add(row);
							
								
						}
					} // for
					
					
					for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
					{
						
						ArrayList<ArrayList<String>> tempData = new ArrayList<>();
						
						
						for(int count = 0; count < tempOutputData.size(); count++)
						{
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
							{
								ArrayList<String> row = new ArrayList<>();
								row.addAll(tempOutputData.get(count));
								row.addAll(tableData.get(fromTableNames.get(tableCount)).get(innerCount));
								
									tempData.add(row);
								
									
							}
						} // for
						
						tempOutputData.clear();
						tempOutputData.addAll(tempData);
						
						
					} // for
					
					outputData.addAll(tempOutputData);
					
					
				} //if
				else
				{
					// only one table
					if(fromTableNames.size() == 1)
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							
							
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							outputData.add(row);
						}
					} //if
					// two tables
					else
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
							{
								
								ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(1));
								ArrayList<String> innerRow = new ArrayList<>();
								for(int index2 = 0; index2 < innerIndices.size(); index2++)
								{
									innerRow.add(tableData.get(fromTableNames.get(1)).get(innerCount).get(innerIndices.get(index2)));
								}
								
								ArrayList<String> nestedRow = new ArrayList<>();
								nestedRow.addAll(row);
								nestedRow.addAll(innerRow);
								outputData.add(nestedRow);								
							}
						} // for
						
					} // else
				}// else
				
				outputData = filterWhereClause(outputData, whereClause);
				
				int sum = 0 ;
				int cnt = 0;
				
				for(int count = 1; count < outputData.size(); count++)
				{
					
					sum += Integer.parseInt(outputData.get(count).get(0));
					cnt++;
					outputData.remove(count);
					count--;
				}
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf((float)(sum) / cnt));
				outputData.add(maxData);
				header.set(0, "avg("+header.get(0)+")");
				outputData.set(0, header);
				displayResults(outputData);
			}
			
			
		}
		//sum
		// select sum() with where conditions
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("sum("))
		{
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
						
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add(fromTableNames.get(0)+"." + columnName);
				outputData.add(header);
				
				Map<String, ArrayList<Integer>> tableNameTocolumnIndicesRequired = new HashMap<>();
				// adding headers
				for(int count = 0; count < fromTableNames.size(); count++)
				{
					ArrayList<Integer> columnIndices = new ArrayList<>();
					
					for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
					{
						if(header.contains(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name)))
						{
							columnIndices.add(name);
						//	headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
							
						}
					}
					tableNameTocolumnIndicesRequired.put(fromTableNames.get(count), columnIndices);
				}
				if(headers.size() == 0)
				{
					throw new Exception("column not found");
				}
				
				
				
				// handles special case
				if(fromTableNames.size() > 2)
				{
					ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tableData.get(fromTableNames.get(0)).get(count));
							row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
							
								tempOutputData.add(row);
							
								
						}
					} // for
					
					
					for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
					{
						
						ArrayList<ArrayList<String>> tempData = new ArrayList<>();
						
						
						for(int count = 0; count < tempOutputData.size(); count++)
						{
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
							{
								ArrayList<String> row = new ArrayList<>();
								row.addAll(tempOutputData.get(count));
								row.addAll(tableData.get(fromTableNames.get(tableCount)).get(innerCount));
								
									tempData.add(row);
								
									
							}
						} // for
						
						
						
						tempOutputData.clear();
						tempOutputData.addAll(tempData);
						
						
					} // for
					
					outputData.addAll(tempOutputData);
					
				} //if
				else
				{
					// only one table
					if(fromTableNames.size() == 1)
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							
							
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							outputData.add(row);
						}
					} //if
					// two tables
					else
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
							{
								
								ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(1));
								ArrayList<String> innerRow = new ArrayList<>();
								for(int index2 = 0; index2 < innerIndices.size(); index2++)
								{
									innerRow.add(tableData.get(fromTableNames.get(1)).get(innerCount).get(innerIndices.get(index2)));
								}
								
								ArrayList<String> nestedRow = new ArrayList<>();
								nestedRow.addAll(row);
								nestedRow.addAll(innerRow);
								outputData.add(nestedRow);								
							}
						} // for
						
					} // else
				}// else
				
				outputData = filterWhereClause(outputData, whereClause);
				
				int sum = 0;
				
				for(int count = 1; count < outputData.size(); count++)
				{
					
					sum += Integer.parseInt(outputData.get(count).get(0));
					outputData.remove(count);
					count--;
				}
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf(sum));
				outputData.add(maxData);
				header.set(0, "sum("+header.get(0)+")");
				outputData.set(0, header);
				displayResults(outputData);
			}
			
					
		}
		// min
		// select min with where clause
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).toLowerCase().contains("min("))
		{
			
			ArrayList<String> headers = new ArrayList<>();
			
			// adding headers
			for(int count = 0; count < fromTableNames.size(); count++)
			{
				for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
				{
					headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
				}
			}
						
			//outputData.add(headers);
			
			String columnName = selectColumnsNames.get(0).substring(4);
			if(columnName.contains("."))
			{
				columnName = columnName.substring(columnName.indexOf(".")+1);
			}
			columnName = columnName.replace(")", "");
			if(columnName.contains(","))
			{
				throw new Exception("multiple aggregate columns not supported");
			}
			else  if(!headers.contains(fromTableNames.get(0)+"." + columnName))
			{
				throw new Exception("column does not exist");
			}
			else if(fromTableNames.size() > 1)
			{
				throw new Exception("multitable aggreagate not supported !");
			}
			else
			{
				
				int index = headers.indexOf(fromTableNames.get(0)+"." + columnName);
				ArrayList<String> header = new ArrayList<>();
				header.add(fromTableNames.get(0)+"." + columnName);
				outputData.add(header);
				
				Map<String, ArrayList<Integer>> tableNameTocolumnIndicesRequired = new HashMap<>();
				// adding headers
				for(int count = 0; count < fromTableNames.size(); count++)
				{
					ArrayList<Integer> columnIndices = new ArrayList<>();
					
					for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
					{
						if(header.contains(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name)))
						{
							columnIndices.add(name);
						//	headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
							
						}
					}
					tableNameTocolumnIndicesRequired.put(fromTableNames.get(count), columnIndices);
				}
				if(headers.size() == 0)
				{
					throw new Exception("column not found");
				}
				
				
				
				// handles special case
				if(fromTableNames.size() > 2)
				{
					ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tableData.get(fromTableNames.get(0)).get(count));
							row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
							
								tempOutputData.add(row);
							
								
						}
					} // for
					
					
					for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
					{
						
						ArrayList<ArrayList<String>> tempData = new ArrayList<>();
						
						
						for(int count = 0; count < tempOutputData.size(); count++)
						{
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
							{
								ArrayList<String> row = new ArrayList<>();
								row.addAll(tempOutputData.get(count));
								row.addAll(tableData.get(fromTableNames.get(tableCount)).get(innerCount));
								
									tempData.add(row);
								
									
							}
						} // for
						
						
						
						tempOutputData.clear();
						tempOutputData.addAll(tempData);
						
						
					} // for
					
					outputData.addAll(tempOutputData);
					
					
				} //if
				else
				{
					// only one table
					if(fromTableNames.size() == 1)
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							
							
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							outputData.add(row);
						}
					} //if
					// two tables
					else
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							
							ArrayList<Integer> indices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(0));
							ArrayList<String> row = new ArrayList<>();
							for(int index2 = 0; index2 < indices.size(); index2++)
							{
								row.add(tableData.get(fromTableNames.get(0)).get(count).get(indices.get(index2)));
							}
							
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
							{
								
								ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(1));
								ArrayList<String> innerRow = new ArrayList<>();
								for(int index2 = 0; index2 < innerIndices.size(); index2++)
								{
									innerRow.add(tableData.get(fromTableNames.get(1)).get(innerCount).get(innerIndices.get(index2)));
								}
								
								ArrayList<String> nestedRow = new ArrayList<>();
								nestedRow.addAll(row);
								nestedRow.addAll(innerRow);
								outputData.add(nestedRow);								
							}
						} // for
						
					} // else
				}// else
				
				outputData = filterWhereClause(outputData, whereClause);
				
				int min = Integer.MAX_VALUE;
				
				for(int count = 1; count < outputData.size(); count++)
				{
					if(Integer.parseInt(outputData.get(count).get(0)) < min)
						min = Integer.parseInt(outputData.get(count).get(0));
					outputData.remove(count);
					count--;
				}
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf(min));
				outputData.add(maxData);
				header.set(0, "min("+header.get(0)+")");
				outputData.set(0, header);
				displayResults(outputData);
			}
			
			
		} // min
		
		/**
		 * handle from here
		 */
		
		else // select one or more columns
		{
			ArrayList<String> headers = new ArrayList<>();
			Map<String, ArrayList<Integer>> tableNameTocolumnIndicesRequired = new HashMap<>();
			// adding headers
			/**
			 * only supported formats : select table1.col1, table2.col2 / select col1, col2
			 */

		
				// adding headers
				for(int count = 0; count < fromTableNames.size(); count++)
				{
					for(int name = 0; name < tableNameToColumns.get(fromTableNames.get(count)).size(); name ++)
					{
						headers.add(fromTableNames.get(count) + "." + tableNameToColumns.get(fromTableNames.get(count)).get(name));
					}
				}
				outputData.add(headers);
				
				
				Map<ArrayList<String>, Boolean> mapForDistinct = new HashMap<>();
				
				// handles special case
				if(fromTableNames.size() > 2)
				{
					ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
					for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
					{
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							ArrayList<String> row = new ArrayList<>();
							row.addAll(tableData.get(fromTableNames.get(0)).get(count));
							row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
							if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
							{
								tempOutputData.add(row);
								mapForDistinct.put(row, new Boolean(true));
							}
							else if(isDistinctFound == 0)
							{
								tempOutputData.add(row);
							}
								
						}
					} // for
					
					
					for(int tableCount = 2; tableCount < fromTableNames.size(); tableCount++)
					{
						
						ArrayList<ArrayList<String>> tempData = new ArrayList<>();
						
						
						for(int count = 0; count < tempOutputData.size(); count++)
						{
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(tableCount)).size(); innerCount++)
							{
								ArrayList<String> row = new ArrayList<>();
								row.addAll(tempOutputData.get(count));
								row.addAll(tableData.get(fromTableNames.get(tableCount)).get(innerCount));
								if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
								{
									tempData.add(row);
									mapForDistinct.put(row, new Boolean(true));
								}
								else if(isDistinctFound == 0)
								{
									tempData.add(row);
								}
									
							}
						} // for
						
						tempOutputData.clear();
						tempOutputData.addAll(tempData);
						
					} // for
					
					outputData.addAll(tempOutputData);
					
					
				} //if size > 2
				else
				{
					// only one table
					if(fromTableNames.size() == 1)
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							// distinct
							if(isDistinctFound == 1 && mapForDistinct.get(tableData.get(fromTableNames.get(0)).get(count)) == null)
							{
								outputData.add(tableData.get(fromTableNames.get(0)).get(count));
								mapForDistinct.put(tableData.get(fromTableNames.get(0)).get(count), new Boolean(true));
								
							}
							else if(isDistinctFound == 0)
							{
								outputData.add(tableData.get(fromTableNames.get(0)).get(count));
							}
							
						}
					} //if
					// two tables
					else
					{
						for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
						{
							for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
							{
								ArrayList<String> row = new ArrayList<>();
								row.addAll(tableData.get(fromTableNames.get(0)).get(count));
								row.addAll(tableData.get(fromTableNames.get(1)).get(innerCount));
								if(isDistinctFound == 1 &&   mapForDistinct.get(row) == null)
								{
									outputData.add(row);
									mapForDistinct.put(row, new Boolean(true));
								}
								else if(isDistinctFound == 0)
								{
									outputData.add(row);
								}
									
							}
						} // for
						
					} // else
				}// else // else size < 2
			
			outputData = filterWhereClause(outputData, whereClause);
			
			ArrayList<Integer> indicesRequired = new ArrayList<Integer>();
			
			if(!selectColumnsNames.get(0).contains("."))
			{	
				for(int count = 0; count < outputData.get(0).size(); count++)
				{
						for(int innerCount = 0; innerCount < selectColumnsNames.size(); innerCount++)
						{
							if(outputData.get(0).get(count).contains("."+selectColumnsNames.get(innerCount)))
							{
								indicesRequired.add(count);
								break;
							}
						}
				}
			}
			else
			{
				for(int count = 0; count < outputData.get(0).size(); count++)
				{
						for(int innerCount = 0; innerCount < selectColumnsNames.size(); innerCount++)
						{
							if(outputData.get(0).get(count).equals(selectColumnsNames.get(innerCount)))
							{
								indicesRequired.add(count);
								break;
							}
						}
				}
			}
			if(indicesRequired.size() == 0 || indicesRequired.size() < selectColumnsNames.size())
			{
				throw new Exception("column not found");
			}
			
			ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
			
			for(int count = 0; count < outputData.size(); count++)
			{
				ArrayList<String> row = new ArrayList<String>();
				for(int innerCount = 0; innerCount < indicesRequired.size(); innerCount++)
				{
					row.add(outputData.get(count).get(indicesRequired.get(innerCount)));
				}
				tempOutputData.add(row);
			}
			
			
			outputData.clear();
			outputData = tempOutputData;
			
			if(isDistinctFound == 1)
				displayDistinctResults(outputData);
			else 
				displayResults(outputData);
		
			
		
		}// else
		
		// TODO Auto-generated method stub
		
	}



	private ArrayList<ArrayList<String>> filterWhereClause(
			ArrayList<ArrayList<String>> outputData, String whereClause) throws Exception
			{
		// TODO Auto-generated method stub
		ArrayList<ArrayList<String>> tempOutputData = new ArrayList<>();
		tempOutputData.addAll(outputData);
		
		
		int numberOfDots = 0;
		for( int i=0; i<whereClause.length(); i++ ) {
		    if( whereClause.charAt(i) == '.' ) {
		    	numberOfDots++;
		    } 
		}
		
		
		int isJoinCondition = 0;
		if((whereClause.contains("AND") || whereClause.contains("OR") ) && !(whereClause.contains("AND") && whereClause.contains("OR")))
		{
			String conditions[] = whereClause.split("where");
			String condition = conditions[1];
			String []joins;
			if(condition.contains("AND"))
			{
				 joins = condition.split("AND");
				
			}
			else
			{
				 joins = condition.split("OR");
			}
			int OneDot = 0;
			for( int i=0; i<joins[0].length(); i++ ) {
			    if( joins[0].charAt(i) == '.' ) {
			    	OneDot++;
			    } 
			}
			int TwoDot = 0;
			for( int i=0; i<joins[1].length(); i++ ) {
			    if( joins[1].charAt(i) == '.' ) {
			    	TwoDot++;
			    } 
			}
			
			if(OneDot == 2 && TwoDot == 2)
			{
				isJoinCondition = 1;
			}
			
			
		}
		else
		{
			
		}
		
		
		
		
		// where clause does not contain AND and OR
		if(!whereClause.contains("AND") && !whereClause.contains("OR") && (!whereClause.contains(".") || numberOfDots == 1))
		{
			String operation = "";
			String operator = "";
			if(whereClause.contains("=") && !whereClause.contains("<=") &&  !whereClause.contains(">=") &&  !whereClause.contains("!="))
			{
				operation = "EQUAL";
				operator = "=";
			}
			else if(whereClause.contains("<") && !whereClause.contains("<="))
			{
				operation = "LESS";
				operator = "<";
			}
			else if(whereClause.contains("<="))
			{
				operation = "LESSEQUAL";
				operator = "<=";
			}
			else if(whereClause.contains(">") && !whereClause.contains(">="))
			{
				operation = "GREATER";
				operator = ">";
			}
			else if(whereClause.contains(">="))
			{
				operation = "GREATEREQUAL";
				operator = ">=";
			}
			else if(whereClause.contains(" != "))
			{
				operation = "NOTEQUAL";
				operator = "!=";
			}
			
			if(operation == "")
			{
				throw new Exception("opeartor not supported !");
			}
			
			String []stringTokenizer = whereClause.split("where");
			
			String condition = stringTokenizer[1];
			
			StringTokenizer stringTokenizer2 = new StringTokenizer(condition, operator);
			
			String columnName = stringTokenizer2.nextToken();
			columnName = columnName.replaceAll("\\s+","");
			String value = stringTokenizer2.nextToken();
			value = value.replaceAll("\\s+","");
			int columnIndex = -1;
			for(int count = 0; count < outputData.get(0).size(); count++)
			{
				if(columnName.contains("."))
				{
					if(outputData.get(0).get(count).equals(columnName))
					{
						columnIndex = count;
						break;
					}
				}
				else
				{
					if(outputData.get(0).get(count).contains("."+columnName))
					{
						columnIndex = count;
						break;
					}
				}
				
			}
			if(columnIndex == -1)
			{
				throw new Exception("column not found !");
			}
			
			
			for(int count = 1; count < outputData.size(); count++)
			{
				
				switch(operation)
				{
					case "EQUAL":
					if(!outputData.get(count).get(columnIndex).equals(value))
					{
						outputData.remove(count);
						count--;
					}
					break;
					
					case "LESS":
						if(Integer.parseInt(outputData.get(count).get(columnIndex)) >= Integer.parseInt(value))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "LESSEQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndex)) > Integer.parseInt(value))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
					case "GREATER":
						if(Integer.parseInt(outputData.get(count).get(columnIndex)) <= Integer.parseInt(value))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "GREATEREQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndex)) < Integer.parseInt(value))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "NOTEQUAL":
						if(outputData.get(count).get(columnIndex).equals(value))
						{
							outputData.remove(count);
							count--;
						}
						break;
					
				}
				
			}
			
		}
		// single AND
		else if(whereClause.contains("AND") && !whereClause.contains("OR") && isJoinCondition == 0)
		{
			String operationOne = "";
			String operatorOne = "";
			String operationTwo = "";
			String operatorTwo = "";
			
			String []stringTokenizer = whereClause.split("where");
			String temp = stringTokenizer[1];
			String[] stringTokenizerWhere = temp.split("AND");
			
			String temp1 = stringTokenizerWhere[0];
			String temp2 = stringTokenizerWhere[1];
		
				
			
			String whereClauseOne = temp1;
			String whereClauseSecond = temp2;
			
		
			
			if(whereClauseOne.contains("=") && !whereClauseOne.contains("<=") &&  !whereClauseOne.contains(">=") &&  !whereClauseOne.contains("!="))
			{
				operationOne = "EQUAL";
				operatorOne = "=";
			}
			else if(whereClauseOne.contains("<") && !whereClauseOne.contains("<="))
			{
				operationOne = "LESS";
				operatorOne = "<";
			}
			else if(whereClauseOne.contains("<="))
			{
				operationOne = "LESSEQUAL";
				operatorOne = "<=";
			}
			else if(whereClauseOne.contains(">") && !whereClauseOne.contains(">="))
			{
				operationOne = "GREATER";
				operatorOne = ">";
			}
			else if(whereClauseOne.contains(">="))
			{
				operationOne = "GREATEREQUAL";
				operatorOne = ">=";
			}
			else if(whereClauseOne.contains(" != "))
			{
				operationOne = "NOTEQUAL";
				operatorOne = "!=";
			}
			
			if(operationOne == "")
			{
				throw new Exception("opeartor not supported !");
			}
			
			
			
			StringTokenizer stringTokenizer2 = new StringTokenizer(whereClauseOne, operatorOne);
			
			String columnNameOne = stringTokenizer2.nextToken();
			columnNameOne = columnNameOne.replaceAll("\\s+","");
			String valueOne = stringTokenizer2.nextToken();
			valueOne = valueOne.replaceAll("\\s+","");
			
			int columnIndexOne = -1;
			for(int count = 0; count < outputData.get(0).size(); count++)
			{
				if(columnNameOne.contains("."))
				{
					if(outputData.get(0).get(count).equals(columnNameOne))
					{
						columnIndexOne = count;
						break;
					}
				}
				else
				{
					if(outputData.get(0).get(count).contains("."+columnNameOne))
					{
						columnIndexOne = count;
						break;
					}
				}
			}
			if(columnIndexOne == -1)
			{
				throw new Exception("column not found !");
			}
			
			for(int count = 1; count < outputData.size(); count++)
			{
				
				switch(operationOne)
				{
					case "EQUAL":
					if(!outputData.get(count).get(columnIndexOne).equals(valueOne))
					{
						outputData.remove(count);
						count--;
					}
					break;
					
					case "LESS":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) >= Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
					case "LESSEQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) > Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
					case "GREATER":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) <= Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "GREATEREQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) < Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "NOTEQUAL":
						if(outputData.get(count).get(columnIndexOne).equals(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
					
				}
				
			} // for
			
			
			if(whereClauseSecond.contains("=") && !whereClauseSecond.contains("<=") &&  !whereClauseSecond.contains(">=") &&  !whereClauseSecond.contains("!="))
			{
				operationTwo = "EQUAL";
				operatorTwo = "=";
			}
			else if(whereClauseSecond.contains("<") && !whereClauseSecond.contains("<="))
			{
				operationTwo = "LESS";
				operatorTwo = "<";
			}
			else if(whereClauseSecond.contains("<="))
			{
				operationTwo = "LESSEQUAL";
				operatorTwo = "<=";
			}
			else if(whereClauseSecond.contains(">") && !whereClauseSecond.contains(">="))
			{
				operationTwo = "GREATER";
				operatorTwo = ">";
			}
			else if(whereClauseSecond.contains(">="))
			{
				operationTwo = "GREATEREQUAL";
				operatorTwo = ">=";
			}
			else if(whereClauseSecond.contains(" != "))
			{
				operationTwo = "NOTEQUAL";
				operatorTwo = "!=";
			}
			
			if(operationTwo == "")
			{
				throw new Exception("opeartor not supported !");
			}
			
			
			
			StringTokenizer stringTokenizer3 = new StringTokenizer(whereClauseSecond, operatorTwo);
			
			String columnNameTwo = stringTokenizer3.nextToken();
			columnNameTwo = columnNameTwo.replaceAll("\\s+","");
			String valueTwo = stringTokenizer3.nextToken();
			valueTwo = valueTwo.replaceAll("\\s+","");
			
			int columnIndexTwo = -1;
			for(int count = 0; count < outputData.get(0).size(); count++)
			{
				
				if(columnNameTwo.contains("."))
				{
					if(outputData.get(0).get(count).equals(columnNameTwo))
					{
						columnIndexTwo = count;
						break;
					}
				}
				else
				{
					if(outputData.get(0).get(count).contains("."+columnNameTwo))
					{
						columnIndexTwo = count;
						break;
					}
				}
					
				
			}
			if(columnIndexTwo == -1)
			{
				throw new Exception("column not found !");
			}
			
			
			
			
			
			for(int count = 1; count < outputData.size(); count++)
			{
				
				switch(operationTwo)
				{
					case "EQUAL":
					if(!outputData.get(count).get(columnIndexTwo).equals(valueTwo))
					{
						outputData.remove(count);
						count--;
					}
					break;
					
					case "LESS":
						if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) >= Integer.parseInt(valueTwo))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "LESSEQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) > Integer.parseInt(valueTwo))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
					case "GREATER":
						if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) <= Integer.parseInt(valueTwo))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "GREATEREQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) < Integer.parseInt(valueTwo))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "NOTEQUAL":
						if(outputData.get(count).get(columnIndexTwo).equals(valueTwo))
						{
							outputData.remove(count);
							count--;
						}
						break;
					
				}
				
			} // for
			
			
			
			
			
		}
		// signle OR
		else if(whereClause.contains("OR") && !whereClause.contains("AND") && isJoinCondition == 0)
		{
			
			
			String operationOne = "";
			String operatorOne = "";
			String operationTwo = "";
			String operatorTwo = "";
			
			String[] stringTokenizer = whereClause.split("where");
			String temp = stringTokenizer[1];
			String[] stringTokenizerWhere = temp.split("OR");
			
			String temp1 = stringTokenizerWhere[0];
			String temp2 = stringTokenizerWhere[1];
		
				
			
			String whereClauseOne = temp1;
			String whereClauseSecond = temp2;
			
		
			
			if(whereClauseOne.contains("=") && !whereClauseOne.contains("<=") &&  !whereClauseOne.contains(">=") &&  !whereClauseOne.contains("!="))
			{
				operationOne = "EQUAL";
				operatorOne = "=";
			}
			else if(whereClauseOne.contains("<") && !whereClauseOne.contains("<="))
			{
				operationOne = "LESS";
				operatorOne = "<";
			}
			else if(whereClauseOne.contains("<="))
			{
				operationOne = "LESSEQUAL";
				operatorOne = "<=";
			}
			else if(whereClauseOne.contains(">") && !whereClauseOne.contains(">="))
			{
				operationOne = "GREATER";
				operatorOne = ">";
			}
			else if(whereClauseOne.contains(">="))
			{
				operationOne = "GREATEREQUAL";
				operatorOne = ">=";
			}
			else if(whereClauseOne.contains(" != "))
			{
				operationOne = "NOTEQUAL";
				operatorOne = "!=";
			}
			
			if(operationOne == "")
			{
				throw new Exception("opeartor not supported !");
			}
			
			
			
			StringTokenizer stringTokenizer2 = new StringTokenizer(whereClauseOne, operatorOne);
			
			String columnNameOne = stringTokenizer2.nextToken();
			columnNameOne = columnNameOne.replaceAll("\\s+","");
			String valueOne = stringTokenizer2.nextToken();
			valueOne = valueOne.replaceAll("\\s+","");
			
			int columnIndexOne = -1;
			for(int count = 0; count < outputData.get(0).size(); count++)
			{
				if(columnNameOne.contains("."))
				{
					if(outputData.get(0).get(count).equals(columnNameOne))
					{
						columnIndexOne = count;
						break;
					}
				}
				else
				{
					if(outputData.get(0).get(count).contains("."+columnNameOne))
					{
						columnIndexOne = count;
						break;
					}
				}
			}
			if(columnIndexOne == -1)
			{
				throw new Exception("column not found !");
			}
			
			
			
			
			
			for(int count = 1; count < outputData.size(); count++)
			{
				
				switch(operationOne)
				{
					case "EQUAL":
					if(!outputData.get(count).get(columnIndexOne).equals(valueOne))
					{
						outputData.remove(count);
						count--;
					}
					break;
					
					case "LESS":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) >= Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "LESSEQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) > Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
					case "GREATER":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) <= Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "GREATEREQUAL":
						if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) < Integer.parseInt(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						
					case "NOTEQUAL":
						if(outputData.get(count).get(columnIndexOne).equals(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
					
				}
				
			} // for
			
			
			if(whereClauseSecond.contains("=") && !whereClauseSecond.contains("<=") &&  !whereClauseSecond.contains(">=") &&  !whereClauseSecond.contains("!="))
			{
				operationTwo = "EQUAL";
				operatorTwo = "=";
			}
			else if(whereClauseSecond.contains("<") && !whereClauseSecond.contains("<="))
			{
				operationTwo = "LESS";
				operatorTwo = "<";
			}
			else if(whereClauseSecond.contains("<="))
			{
				operationTwo = "LESSEQUAL";
				operatorTwo = "<=";
			}
			else if(whereClauseSecond.contains(">") && !whereClauseSecond.contains(">="))
			{
				operationTwo = "GREATER";
				operatorTwo = ">";
			}
			else if(whereClauseSecond.contains(">="))
			{
				operationTwo = "GREATEREQUAL";
				operatorTwo = ">=";
			}
			else if(whereClauseSecond.contains(" != "))
			{
				operationTwo = "NOTEQUAL";
				operatorTwo = "!=";
			}
			
			if(operationTwo == "")
			{
				throw new Exception("opeartor not supported !");
			}
			
			
			
			StringTokenizer stringTokenizer3 = new StringTokenizer(whereClauseSecond, operatorTwo);
			
			String columnNameTwo = stringTokenizer3.nextToken();
			columnNameTwo = columnNameTwo.replaceAll("\\s+","");
			String valueTwo = stringTokenizer3.nextToken();
			valueTwo = valueTwo.replaceAll("\\s+","");
			
			int columnIndexTwo = -1;
			for(int count = 0; count < outputData.get(0).size(); count++)
			{
				if(columnNameTwo.contains("."))
				{
					if(outputData.get(0).get(count).equals(columnNameTwo))
					{
						columnIndexTwo = count;
						break;
					}
				}
				else
				{
					if(outputData.get(0).get(count).contains("."+columnNameTwo))
					{
						columnIndexTwo = count;
						break;
					}
				}
			}
			if(columnIndexTwo == -1)
			{
				throw new Exception("column not found !");
			}
			
			
			
			
			
			for(int count = 1; count < tempOutputData.size(); count++)
			{
				
				switch(operationTwo)
				{
					case "EQUAL":
					if(!tempOutputData.get(count).get(columnIndexTwo).equals(valueTwo))
					{
						tempOutputData.remove(count);
						count--;
					}
					break;
					
					case "LESS":
						if(Integer.parseInt(tempOutputData.get(count).get(columnIndexTwo)) >= Integer.parseInt(valueTwo))
						{
							tempOutputData.remove(count);
							count--;
						}
						break;
						
						
					case "LESSEQUAL":
						if(Integer.parseInt(tempOutputData.get(count).get(columnIndexTwo)) > Integer.parseInt(valueTwo))
						{
							tempOutputData.remove(count);
							count--;
						}
						break;
						
					case "GREATER":
						if(Integer.parseInt(tempOutputData.get(count).get(columnIndexTwo)) <= Integer.parseInt(valueTwo))
						{
							tempOutputData.remove(count);
							count--;
						}
						break;
						
						
					case "GREATEREQUAL":
						if(Integer.parseInt(tempOutputData.get(count).get(columnIndexTwo)) < Integer.parseInt(valueTwo))
						{
							tempOutputData.remove(count);
							count--;
						}
						break;
						
						
					case "NOTEQUAL":
						if(tempOutputData.get(count).get(columnIndexTwo).equals(valueTwo))
						{
							tempOutputData.remove(count);
							count--;
						}
						break;
					
				}
				
			} // for
			
			Set<ArrayList<String>> test = new LinkedHashSet<>();
			
			test.add(outputData.get(0));
			
			for(int i= 1; i < outputData.size(); i++)
			{
				test.add(outputData.get(i));
			}
			for(int i= 1; i < tempOutputData.size(); i++)
			{
				test.add(tempOutputData.get(i));
			}
			
			
			
			outputData = new ArrayList<>(test);
		}
		else if(whereClause.contains(".") && !whereClause.contains("AND") && !whereClause.contains("OR") && numberOfDots == 2)
		{
			String[] stringTokenizer = whereClause.split("where");
			String condition = stringTokenizer[1];
			if(!condition.contains("="))
			{
				throw new Exception("only euqlaity join supported");
			}
			
		    String columns[] = condition.split("=");
		    String column1 = columns[0];
		    column1 = column1.replaceAll("\\s+","");
		    String column2 = columns[1];
		    column2 = column2.replaceAll("\\s+","");
		    
		    
		    ArrayList<String> headers = outputData.get(0);
		    
		    if(!headers.contains(column1))
		    {
		    	throw new Exception("column not found");
		    }
		    else if(!headers.contains(column2))
		    {
		    	throw new Exception("column not found");
		    }
		    
		    int index1 = headers.indexOf(column1);
		    int index2 = headers.indexOf(column2);
		    
		    
		    for(int count = 1; count < outputData.size(); count++)
		    {
		    	if(!outputData.get(count).get(index1).equals(outputData.get(count).get(index2)))
		    	{
		    		outputData.remove(count);
		    		count--;
		    	}
		    }
		    
		    for(int count = 0; count < outputData.size(); count++)
		    {
		    	outputData.get(count).remove(index2);
		    }
		    
			
			
			
		}
		// combination of AND and OR
		else if(whereClause.contains("OR") && whereClause.contains("AND"))
		{
			
			
			
			int indexOfOr = whereClause.indexOf("OR");
			int indexOfAnd = whereClause.indexOf("AND");
			
			
			// AND before OR
			if(indexOfAnd < indexOfOr)
			{
				String operationOne = "";
				String operatorOne = "";
				String operationTwo = "";
				String operatorTwo = "";
				String operationThree = "";
				String operatorThree = "";
				
				String []stringTokenizer = whereClause.split("where");
				String temp = stringTokenizer[1];
				String[] stringTokenizerWhere = temp.split("AND");
				
				String temp1 = stringTokenizerWhere[0];
				String temp2 = stringTokenizerWhere[1];
			
				String []orClauses = temp2.split("OR"); 	
				
				String whereClauseOne = temp1;
				String whereClauseSecond = orClauses[0];
				String whereClauseThird = orClauses[1];
				
			
				
				if(whereClauseOne.contains("=") && !whereClauseOne.contains("<=") &&  !whereClauseOne.contains(">=") &&  !whereClauseOne.contains("!="))
				{
					operationOne = "EQUAL";
					operatorOne = "=";
				}
				else if(whereClauseOne.contains("<") && !whereClauseOne.contains("<="))
				{
					operationOne = "LESS";
					operatorOne = "<";
				}
				else if(whereClauseOne.contains("<="))
				{
					operationOne = "LESSEQUAL";
					operatorOne = "<=";
				}
				else if(whereClauseOne.contains(">") && !whereClauseOne.contains(">="))
				{
					operationOne = "GREATER";
					operatorOne = ">";
				}
				else if(whereClauseOne.contains(">="))
				{
					operationOne = "GREATEREQUAL";
					operatorOne = ">=";
				}
				else if(whereClauseOne.contains(" != "))
				{
					operationOne = "NOTEQUAL";
					operatorOne = "!=";
				}
				
				if(operationOne == "")
				{
					throw new Exception("opeartor not supported !");
				}
				
				
				
				StringTokenizer stringTokenizer2 = new StringTokenizer(whereClauseOne, operatorOne);
				
				String columnNameOne = stringTokenizer2.nextToken();
				columnNameOne = columnNameOne.replaceAll("\\s+","");
				String valueOne = stringTokenizer2.nextToken();
				valueOne = valueOne.replaceAll("\\s+","");
				
				int columnIndexOne = -1;
				for(int count = 0; count < outputData.get(0).size(); count++)
				{
					
					if(columnNameOne.contains("."))
					{
						if(outputData.get(0).get(count).equals(columnNameOne))
						{
							columnIndexOne = count;
							break;
						}
					}
					else
					{
						if(outputData.get(0).get(count).contains("."+columnNameOne))
						{
							columnIndexOne = count;
							break;
						}
					}
					
				}
				if(columnIndexOne == -1)
				{
					throw new Exception("column not found !");
				}
				
				for(int count = 1; count < outputData.size(); count++)
				{
					
					switch(operationOne)
					{
						case "EQUAL":
						if(!outputData.get(count).get(columnIndexOne).equals(valueOne))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						case "LESS":
							if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) >= Integer.parseInt(valueOne))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "LESSEQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) > Integer.parseInt(valueOne))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
						case "GREATER":
							if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) <= Integer.parseInt(valueOne))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "GREATEREQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexOne)) < Integer.parseInt(valueOne))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "NOTEQUAL":
							if(outputData.get(count).get(columnIndexOne).equals(valueOne))
							{
								outputData.remove(count);
								count--;
							}
							break;
						
					}
					
				} // for
				
				
				if(whereClauseSecond.contains("=") && !whereClauseSecond.contains("<=") &&  !whereClauseSecond.contains(">=") &&  !whereClauseSecond.contains("!="))
				{
					operationTwo = "EQUAL";
					operatorTwo = "=";
				}
				else if(whereClauseSecond.contains("<") && !whereClauseSecond.contains("<="))
				{
					operationTwo = "LESS";
					operatorTwo = "<";
				}
				else if(whereClauseSecond.contains("<="))
				{
					operationTwo = "LESSEQUAL";
					operatorTwo = "<=";
				}
				else if(whereClauseSecond.contains(">") && !whereClauseSecond.contains(">="))
				{
					operationTwo = "GREATER";
					operatorTwo = ">";
				}
				else if(whereClauseSecond.contains(">="))
				{
					operationTwo = "GREATEREQUAL";
					operatorTwo = ">=";
				}
				else if(whereClauseSecond.contains(" != "))
				{
					operationTwo = "NOTEQUAL";
					operatorTwo = "!=";
				}
				
				if(operationTwo == "")
				{
					throw new Exception("opeartor not supported !");
				}
				
				
				
				StringTokenizer stringTokenizer3 = new StringTokenizer(whereClauseSecond, operatorTwo);
				
				String columnNameTwo = stringTokenizer3.nextToken();
				columnNameTwo = columnNameTwo.replaceAll("\\s+","");
				String valueTwo = stringTokenizer3.nextToken();
				valueTwo = valueTwo.replaceAll("\\s+","");
				
				
				
				int columnIndexTwo = -1;
				for(int count = 0; count < outputData.get(0).size(); count++)
				{
					if(columnNameTwo.contains("."))
					{
						if(outputData.get(0).get(count).equals(columnNameTwo))
						{
							columnIndexTwo = count;
							break;
						}
					}
					else
					{
						if(outputData.get(0).get(count).contains("."+columnNameTwo))
						{
							columnIndexTwo = count;
							break;
						}
					}
					
				}
				if(columnIndexTwo == -1)
				{
					throw new Exception("column not found !");
				}
				
				
				
				
				
				for(int count = 1; count < outputData.size(); count++)
				{
					
					switch(operationTwo)
					{
						case "EQUAL":
						if(!outputData.get(count).get(columnIndexTwo).equals(valueTwo))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						case "LESS":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) >= Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "LESSEQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) > Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
						case "GREATER":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) <= Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "GREATEREQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) < Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "NOTEQUAL":
							if(outputData.get(count).get(columnIndexTwo).equals(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
						
					}
					
				} // for
				
				if(whereClauseThird.contains("=") && !whereClauseThird.contains("<=") &&  !whereClauseThird.contains(">=") &&  !whereClauseThird.contains("!="))
				{
					operationThree = "EQUAL";
					operatorThree = "=";
				}
				else if(whereClauseThird.contains("<") && !whereClauseThird.contains("<="))
				{
					operationThree = "LESS";
					operatorThree = "<";
				}
				else if(whereClauseThird.contains("<="))
				{
					operationThree = "LESSEQUAL";
					operatorThree = "<=";
				}
				else if(whereClauseThird.contains(">") && !whereClauseThird.contains(">="))
				{
					operationThree = "GREATER";
					operatorThree = ">";
				}
				else if(whereClauseThird.contains(">="))
				{
					operationThree = "GREATEREQUAL";
					operatorThree = ">=";
				}
				else if(whereClauseThird.contains(" != "))
				{
					operationThree = "NOTEQUAL";
					operatorThree = "!=";
				}
				
				if(operationThree == "")
				{
					throw new Exception("opeartor not supported !");
				}
				
				StringTokenizer stringTokenizer4 = new StringTokenizer(whereClauseThird, operatorThree);
				
				String columnNameThree = stringTokenizer4.nextToken();
				columnNameThree = columnNameThree.replaceAll("\\s+","");
				String valueThree = stringTokenizer4.nextToken();
				valueThree = valueThree.replaceAll("\\s+","");
				
				int columnIndexThree = -1;
				for(int count = 0; count < outputData.get(0).size(); count++)
				{
				
					if(columnNameThree.contains("."))
					{
						if(outputData.get(0).get(count).equals(columnNameThree))
						{
							columnIndexThree = count;
							break;
						}
					}
					else
					{
						if(outputData.get(0).get(count).contains("."+columnNameThree))
						{
							columnIndexThree = count;
							break;
						}
					}
					
				}
				if(columnIndexThree == -1)
				{
					throw new Exception("column not found !");
				}
				
				for(int count = 1; count < tempOutputData.size(); count++)
				{
					
					switch(operationThree)
					{
						case "EQUAL":
						if(!tempOutputData.get(count).get(columnIndexThree).equals(valueThree))
						{
							tempOutputData.remove(count);
							count--;
						}
						break;
						
						case "LESS":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexThree)) >= Integer.parseInt(valueThree))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
							
						case "LESSEQUAL":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexThree)) > Integer.parseInt(valueThree))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
						case "GREATER":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexThree)) <= Integer.parseInt(valueThree))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
							
						case "GREATEREQUAL":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexThree)) < Integer.parseInt(valueThree))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
							
						case "NOTEQUAL":
							if(tempOutputData.get(count).get(columnIndexThree).equals(valueThree))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
						
					}
					
				} // for
				
			
				Set<ArrayList<String>> test = new LinkedHashSet<>();
				
				test.add(outputData.get(0));
				
				for(int i= 1; i < outputData.size(); i++)
				{
					test.add(outputData.get(i));
				}
				for(int i= 1; i < tempOutputData.size(); i++)
				{
					test.add(tempOutputData.get(i));
				}
								
				outputData = new ArrayList<>(test);

				
			}
			// OR before AND
			else
			{
				String operationOne = "";
				String operatorOne = "";
				String operationTwo = "";
				String operatorTwo = "";
				String operationThree = "";
				String operatorThree = "";
				
				String []stringTokenizer = whereClause.split("where");
				String temp = stringTokenizer[1];
				String[] stringTokenizerWhere = temp.split("OR");
				
				String temp1 = stringTokenizerWhere[0];
				String temp2 = stringTokenizerWhere[1];
			
				String []orClauses = temp2.split("AND"); 	
				
				String whereClauseOne = temp1;
				String whereClauseSecond = orClauses[0];
				String whereClauseThird = orClauses[1];
				
			
				
				if(whereClauseOne.contains("=") && !whereClauseOne.contains("<=") &&  !whereClauseOne.contains(">=") &&  !whereClauseOne.contains("!="))
				{
					operationOne = "EQUAL";
					operatorOne = "=";
				}
				else if(whereClauseOne.contains("<") && !whereClauseOne.contains("<="))
				{
					operationOne = "LESS";
					operatorOne = "<";
				}
				else if(whereClauseOne.contains("<="))
				{
					operationOne = "LESSEQUAL";
					operatorOne = "<=";
				}
				else if(whereClauseOne.contains(">") && !whereClauseOne.contains(">="))
				{
					operationOne = "GREATER";
					operatorOne = ">";
				}
				else if(whereClauseOne.contains(">="))
				{
					operationOne = "GREATEREQUAL";
					operatorOne = ">=";
				}
				else if(whereClauseOne.contains(" != "))
				{
					operationOne = "NOTEQUAL";
					operatorOne = "!=";
				}
				
				if(operationOne == "")
				{
					throw new Exception("opeartor not supported !");
				}
				
				
				
				StringTokenizer stringTokenizer2 = new StringTokenizer(whereClauseOne, operatorOne);
				
				String columnNameOne = stringTokenizer2.nextToken();
				columnNameOne = columnNameOne.replaceAll("\\s+","");
				String valueOne = stringTokenizer2.nextToken();
				valueOne = valueOne.replaceAll("\\s+","");
				
				int columnIndexOne = -1;
				for(int count = 0; count < outputData.get(0).size(); count++)
				{
					
					if(columnNameOne.contains("."))
					{
						if(outputData.get(0).get(count).equals(columnNameOne))
						{
							columnIndexOne = count;
							break;
						}
					}
					else
					{
						if(outputData.get(0).get(count).contains("."+columnNameOne))
						{
							columnIndexOne = count;
							break;
						}
					}
				}
				if(columnIndexOne == -1)
				{
					throw new Exception("column not found !");
				}
				
				for(int count = 1; count < tempOutputData.size(); count++)
				{
					
					switch(operationOne)
					{
						case "EQUAL":
						if(!tempOutputData.get(count).get(columnIndexOne).equals(valueOne))
						{
							tempOutputData.remove(count);
							count--;
						}
						break;
						
						case "LESS":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexOne)) >= Integer.parseInt(valueOne))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
							
						case "LESSEQUAL":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexOne)) > Integer.parseInt(valueOne))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
						case "GREATER":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexOne)) <= Integer.parseInt(valueOne))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
							
						case "GREATEREQUAL":
							if(Integer.parseInt(tempOutputData.get(count).get(columnIndexOne)) < Integer.parseInt(valueOne))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
							
							
						case "NOTEQUAL":
							if(tempOutputData.get(count).get(columnIndexOne).equals(valueOne))
							{
								tempOutputData.remove(count);
								count--;
							}
							break;
						
					}
					
				} // for
				
				
				if(whereClauseSecond.contains("=") && !whereClauseSecond.contains("<=") &&  !whereClauseSecond.contains(">=") &&  !whereClauseSecond.contains("!="))
				{
					operationTwo = "EQUAL";
					operatorTwo = "=";
				}
				else if(whereClauseSecond.contains("<") && !whereClauseSecond.contains("<="))
				{
					operationTwo = "LESS";
					operatorTwo = "<";
				}
				else if(whereClauseSecond.contains("<="))
				{
					operationTwo = "LESSEQUAL";
					operatorTwo = "<=";
				}
				else if(whereClauseSecond.contains(">") && !whereClauseSecond.contains(">="))
				{
					operationTwo = "GREATER";
					operatorTwo = ">";
				}
				else if(whereClauseSecond.contains(">="))
				{
					operationTwo = "GREATEREQUAL";
					operatorTwo = ">=";
				}
				else if(whereClauseSecond.contains(" != "))
				{
					operationTwo = "NOTEQUAL";
					operatorTwo = "!=";
				}
				
				if(operationTwo == "")
				{
					throw new Exception("opeartor not supported !");
				}
				
				
				
				StringTokenizer stringTokenizer3 = new StringTokenizer(whereClauseSecond, operatorTwo);
				
				String columnNameTwo = stringTokenizer3.nextToken();
				columnNameTwo = columnNameTwo.replaceAll("\\s+","");
				String valueTwo = stringTokenizer3.nextToken();
				valueTwo = valueTwo.replaceAll("\\s+","");
				
				
				
				int columnIndexTwo = -1;
				for(int count = 0; count < outputData.get(0).size(); count++)
				{

					if(columnNameTwo.contains("."))
					{
						if(outputData.get(0).get(count).equals(columnNameTwo))
						{
							columnIndexTwo = count;
							break;
						}
					}
					else
					{
						if(outputData.get(0).get(count).contains("."+columnNameTwo))
						{
							columnIndexTwo = count;
							break;
						}
					}
					
				}
				if(columnIndexTwo == -1)
				{
					throw new Exception("column not found !");
				}
				
				
				
				
				
				for(int count = 1; count < outputData.size(); count++)
				{
					
					switch(operationTwo)
					{
						case "EQUAL":
						if(!outputData.get(count).get(columnIndexTwo).equals(valueTwo))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						case "LESS":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) >= Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "LESSEQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) > Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
						case "GREATER":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) <= Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "GREATEREQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexTwo)) < Integer.parseInt(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "NOTEQUAL":
							if(outputData.get(count).get(columnIndexTwo).equals(valueTwo))
							{
								outputData.remove(count);
								count--;
							}
							break;
						
					}
					
				} // for
				
				if(whereClauseThird.contains("=") && !whereClauseThird.contains("<=") &&  !whereClauseThird.contains(">=") &&  !whereClauseThird.contains("!="))
				{
					operationThree = "EQUAL";
					operatorThree = "=";
				}
				else if(whereClauseThird.contains("<") && !whereClauseThird.contains("<="))
				{
					operationThree = "LESS";
					operatorThree = "<";
				}
				else if(whereClauseThird.contains("<="))
				{
					operationThree = "LESSEQUAL";
					operatorThree = "<=";
				}
				else if(whereClauseThird.contains(">") && !whereClauseThird.contains(">="))
				{
					operationThree = "GREATER";
					operatorThree = ">";
				}
				else if(whereClauseThird.contains(">="))
				{
					operationThree = "GREATEREQUAL";
					operatorThree = ">=";
				}
				else if(whereClauseThird.contains(" != "))
				{
					operationThree = "NOTEQUAL";
					operatorThree = "!=";
				}
				
				if(operationThree == "")
				{
					throw new Exception("opeartor not supported !");
				}
				
				StringTokenizer stringTokenizer4 = new StringTokenizer(whereClauseThird, operatorThree);
				
				String columnNameThree = stringTokenizer4.nextToken();
				columnNameThree = columnNameThree.replaceAll("\\s+","");
				String valueThree = stringTokenizer4.nextToken();
				valueThree = valueThree.replaceAll("\\s+","");
				
				int columnIndexThree = -1;
				for(int count = 0; count < outputData.get(0).size(); count++)
				{
					
					if(columnNameThree.contains("."))
					{
						if(outputData.get(0).get(count).equals(columnNameThree))
						{
							columnIndexThree = count;
							break;
						}
					}
					else
					{
						if(outputData.get(0).get(count).contains("."+columnNameThree))
						{
							columnIndexThree = count;
							break;
						}
					}
				}
				if(columnIndexThree == -1)
				{
					throw new Exception("column not found !");
				}
				
				for(int count = 1; count < outputData.size(); count++)
				{
					
					switch(operationThree)
					{
						case "EQUAL":
						if(!outputData.get(count).get(columnIndexThree).equals(valueThree))
						{
							outputData.remove(count);
							count--;
						}
						break;
						
						case "LESS":
							if(Integer.parseInt(outputData.get(count).get(columnIndexThree)) >= Integer.parseInt(valueThree))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "LESSEQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexThree)) > Integer.parseInt(valueThree))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
						case "GREATER":
							if(Integer.parseInt(outputData.get(count).get(columnIndexThree)) <= Integer.parseInt(valueThree))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "GREATEREQUAL":
							if(Integer.parseInt(outputData.get(count).get(columnIndexThree)) < Integer.parseInt(valueThree))
							{
								outputData.remove(count);
								count--;
							}
							break;
							
							
						case "NOTEQUAL":
							if(outputData.get(count).get(columnIndexThree).equals(valueThree))
							{
								outputData.remove(count);
								count--;
							}
							break;
						
					}
					
				} // for
				
			
				Set<ArrayList<String>> test = new LinkedHashSet<>();
				
				test.add(outputData.get(0));
				
				for(int i= 1; i < outputData.size(); i++)
				{
					test.add(outputData.get(i));
				}
				for(int i= 1; i < tempOutputData.size(); i++)
				{
					test.add(tempOutputData.get(i));
				}
								
				outputData = new ArrayList<>(test);

				
			}
			
			
		}
		else if(isJoinCondition == 1 && whereClause.contains("AND"))
		{

			String[] stringTokenizer = whereClause.split("where");
			String condition = stringTokenizer[1];
			if(!condition.contains("="))
			{
				throw new Exception("only euqlaity join supported");
			}
			
			String joins[] = condition.split("AND");
			
			
		    String columns[] = joins[0].split("=");
		    String column1 = columns[0];
		    column1 = column1.replaceAll("\\s+","");
		    String column2 = columns[1];
		    column2 = column2.replaceAll("\\s+","");
		    
		    columns = joins[1].split("=");
		    String column3 = columns[0];
		    column3 = column3.replaceAll("\\s+","");
		    String column4 = columns[1];
		    column4 = column4.replaceAll("\\s+","");
		    
		    
		    
		    ArrayList<String> headers = outputData.get(0);
		    
		    if(!headers.contains(column1))
		    {
		    	throw new Exception("column not found");
		    }
		    else if(!headers.contains(column2))
		    {
		    	throw new Exception("column not found");
		    }
		    else if(!headers.contains(column3))
		    {
		    	throw new Exception("column not found");
		    }
		    else if(!headers.contains(column4))
		    {
		    	throw new Exception("column not found");
		    }
		    
		    int index1 = headers.indexOf(column1);
		    int index2 = headers.indexOf(column2);
		    int index3 = headers.indexOf(column3);
		    int index4 = headers.indexOf(column4);
		    
		    
		    for(int count = 1; count < outputData.size(); count++)
		    {
		    	if(!outputData.get(count).get(index1).equals(outputData.get(count).get(index2)))
		    	{
		    		outputData.remove(count);
		    		count--;
		    	}
		    	else if(!outputData.get(count).get(index3).equals(outputData.get(count).get(index4)))
		    	{
		    		outputData.remove(count);
		    		count--;
		    	}
		    }
		    
		    if(index4  > index2)
		    {
		    	for(int count = 0; count < outputData.size(); count++)
			    {
			    	outputData.get(count).remove(index2);
			    	outputData.get(count).remove(index4-1);
			    }
		    }
		    else if( index4 == index2)
		    {
		    	for(int count = 0; count < outputData.size(); count++)
			    {
			    	outputData.get(count).remove(index2);
			    }
			   
		    }
		    else
		    {
		    	for(int count = 0; count < outputData.size(); count++)
			    {
			    	outputData.get(count).remove(index4);
			    	outputData.get(count).remove(index2-1);
			    }
			    
		    }
		    
		    

			
		}
		else if( isJoinCondition == 1 && whereClause.contains("OR"))
		{
			String[] stringTokenizer = whereClause.split("where");
			String condition = stringTokenizer[1];
			if(!condition.contains("="))
			{
				throw new Exception("only euqlaity join supported");
			}
			
			String joins[] = condition.split("OR");
			
		    String columns[] = joins[0].split("=");
		    String column1 = columns[0];
		    column1 = column1.replaceAll("\\s+","");
		    String column2 = columns[1];
		    column2 = column2.replaceAll("\\s+","");
		    
		    columns = joins[1].split("=");
		    String column3 = columns[0];
		    column3 = column3.replaceAll("\\s+","");
		    String column4 = columns[1];
		    column4 = column4.replaceAll("\\s+","");
		    
		    
		    
		    ArrayList<String> headers = outputData.get(0);
		    
		    if(!headers.contains(column1))
		    {
		    	throw new Exception("column not found");
		    }
		    else if(!headers.contains(column2))
		    {
		    	throw new Exception("column not found");
		    }
		    else if(!headers.contains(column3))
		    {
		    	throw new Exception("column not found");
		    }
		    else if(!headers.contains(column4))
		    {
		    	throw new Exception("column not found");
		    }
		    
		    int index1 = headers.indexOf(column1);
		    int index2 = headers.indexOf(column2);
		    int index3 = headers.indexOf(column3);
		    int index4 = headers.indexOf(column4);
		    
		    
		    for(int count = 1; count < outputData.size(); count++)
		    {
		    	if(!outputData.get(count).get(index1).equals(outputData.get(count).get(index2)) && !outputData.get(count).get(index3).equals(outputData.get(count).get(index4)))
		    	{
		    		outputData.remove(count);
		    		count--;
		    	}
		    	
		    }
		    
		    if(index4  > index2)
		    {
		    	for(int count = 0; count < outputData.size(); count++)
			    {
			    	outputData.get(count).remove(index2);
			    	outputData.get(count).remove(index4-1);
			    }
		    }
		    else if( index4 == index2)
		    {
		    	for(int count = 0; count < outputData.size(); count++)
			    {
			    	outputData.get(count).remove(index2);
			    }
			   
		    }
		    else
		    {
		    	for(int count = 0; count < outputData.size(); count++)
			    {
			    	outputData.get(count).remove(index4);
			    	outputData.get(count).remove(index2-1);
			    }
			    
		    }
		    
		    
		}
		return outputData;
	}

}
