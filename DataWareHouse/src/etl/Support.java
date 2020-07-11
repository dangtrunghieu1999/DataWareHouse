package etl;

import org.apache.commons.lang.StringUtils;

public class Support {

	static public String filePath(String source, String file_name) {
		StringBuffer sourceFile = new StringBuffer(source);
		sourceFile.append("/");
		sourceFile.append(file_name);
		String filePath = sourceFile.toString();
		return filePath;
		
	}
	
	static public String convertQuery(String field) {
		String [] headerRow = field.split(",");
		
		String questionmarks = StringUtils.repeat("?,", headerRow.length);
		
		questionmarks = (String) questionmarks.subSequence(0, questionmarks
				.length() - 1);
		String query = LoadData.SQL_INSERT.replaceFirst(LoadData.TABLE_REGEX, "Student");
		
		query = query.replaceFirst(LoadData.VALUES_REGEX, questionmarks);
		return query;
	}
}
