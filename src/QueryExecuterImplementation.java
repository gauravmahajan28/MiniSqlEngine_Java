import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


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
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).contains("max("))
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
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).contains("sum("))
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
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).contains("avg("))
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
				
				int avg = 0;
				int rowCount = 0;
				
				for(int count = 0; count < tableData.get(fromTableNames.get(0)).size(); count++)
				{
					avg += Integer.parseInt(tableData.get(fromTableNames.get(0)).get(count).get(index));
					rowCount++;
						
				}
				float average = (float)avg / rowCount;
				ArrayList<String> maxData = new ArrayList<>();
				maxData.add(String.valueOf(average));
				outputData.add(maxData);
				displayResults(outputData);
			}
			
			
		}
		// min
		else if(selectColumnsNames.size() == 1 && selectColumnsNames.get(0).contains("min("))
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
		
		else if(isDistinctFound != 1)  // select one or more columns
		{
			ArrayList<String> headers = new ArrayList<>();
			Map<String, ArrayList<Integer>> tableNameTocolumnIndicesRequired = new HashMap<>();
			// adding headers
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
			if(headers.size() == 0 || headers.size() != selectColumnsNames.size())
			{
				throw new Exception("column not found");
			}
			
			outputData.add(headers);
			
			// handles special case
			if(fromTableNames.size() > 2)
			{
				
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
							row.add(tableData.get(fromTableNames.get(0)).get(count).get(index));
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
							row.add(tableData.get(fromTableNames.get(0)).get(count).get(index));
						}
						
						for(int innerCount = 0; innerCount < tableData.get(fromTableNames.get(1)).size(); innerCount++)
						{
							
							ArrayList<Integer> innerIndices = tableNameTocolumnIndicesRequired.get(fromTableNames.get(1));
							ArrayList<String> innerRow = new ArrayList<>();
							for(int index = 0; index < innerIndices.size(); index++)
							{
								innerRow.add(tableData.get(fromTableNames.get(1)).get(count).get(index));
							}
							
							ArrayList<String> nestedRow = new ArrayList<>();
							nestedRow.addAll(row);
							nestedRow.addAll(innerRow);
							outputData.add(nestedRow);								
						}
					} // for
					
				} // else
			}// else
			displayResults(outputData);
		}
		else if(isDistinctFound == 1)
		{
			
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



	public void handleSelectStar(ArrayList<String> fromTableNames,
			ArrayList<String> selectColumnsNames,
			Map<String, ArrayList<ArrayList<String>>> tableData) {
		// TODO Auto-generated method stub
		
		
		
	}

}
