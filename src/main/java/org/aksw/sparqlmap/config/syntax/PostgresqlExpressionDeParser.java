package org.aksw.sparqlmap.config.syntax;

import net.sf.jsqlparser.expression.CastStringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.StringValueDirect;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

public class PostgresqlExpressionDeParser extends ExpressionDeParser {
	
	public PostgresqlExpressionDeParser(SelectDeParser selectDeParser,
			StringBuffer out) {
		super(selectDeParser,out);
	}

	@Override
	public void visit(Column tableColumn) {
		String tableName = "";
		if(tableColumn.getTable().getSchemaName()!=null){
			tableName += "\"" + tableColumn.getTable().getSchemaName() + "\".";
		}
		tableName += "\"" + tableColumn.getTable().getName();
		
	
	        String alias = tableColumn.getTable().getAlias();
	        if(alias !=null){
	        	buffer.append("\"" + alias + "\".");
	        }else    	if (tableName.isEmpty()) {
	        
	            buffer.append("\"" + tableName + "\".");
	        }

	        buffer.append("\"" + tableColumn.getColumnName() + "\" ");
	}
	@Override
	public void visit(StringValue stringValue) {
			if(stringValue instanceof CastStringExpression){
				buffer.append(((CastStringExpression) stringValue).getValue('"'));
			}else 	if(stringValue instanceof StringValueDirect){
	    		buffer.append(stringValue.getValue());
	    	}else{
	    		buffer.append("'" + stringValue.getValue() + "'");
	    	}
	}

}
