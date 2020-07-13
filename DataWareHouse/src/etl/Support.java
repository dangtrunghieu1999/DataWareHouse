package etl;

import org.apache.commons.lang.StringUtils;

public class Support {
	public static final String SQL_INSERT = "INSERT INTO ${table} VALUES (${values})";
	public static final String TABLE_REGEX = "\\$\\{table\\}";
	public static final String VALUES_REGEX = "\\$\\{values\\}";
	
	static public String filePath(String source, String file_name) {
		StringBuffer sourceFile = new StringBuffer(source);
		sourceFile.append("/");
		sourceFile.append(file_name);
		String filePath = sourceFile.toString();
		return filePath;
		
	}
	
	static public String convertQuery(String field, String table_name) {
		String [] headerRow = field.split(",");
		
		String questionmarks = StringUtils.repeat("?,", headerRow.length);
		
		questionmarks = (String) questionmarks.subSequence(0, questionmarks
				.length() - 1);
		String query = SQL_INSERT.replaceFirst(TABLE_REGEX, table_name);
		
		query = query.replaceFirst(VALUES_REGEX, questionmarks);
		return query;
	}
}
