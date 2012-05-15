package org.aksw.sparqlmap.config.syntax;

import java.util.Iterator;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.ColumnReference;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

public class PostgresqlSelectDeparser extends SelectDeParser {
	
	@Override
	public void visit(PlainSelect plainSelect) {
		buffer.append("SELECT ");
		Top top = plainSelect.getTop();
		if (top != null)
			top.toString();
		if (plainSelect.getDistinct() != null) {
			buffer.append("DISTINCT ");
			if (plainSelect.getDistinct().getOnSelectItems() != null) {
				buffer.append("ON (");
				for (Iterator iter = plainSelect.getDistinct().getOnSelectItems().iterator(); iter.hasNext();) {
					SelectItem selectItem = (SelectItem) iter.next();
					selectItem.accept(this);
					if (iter.hasNext()) {
						buffer.append(", ");
					}
				}
				buffer.append(") ");
			}

		}

		for (Iterator iter = plainSelect.getSelectItems().iterator(); iter.hasNext();) {
			SelectItem selectItem = (SelectItem) iter.next();
			selectItem.accept(this);
			if (iter.hasNext()) {
				buffer.append(", ");
			}
		}

		buffer.append(" ");
		
		if (plainSelect.getFromItem() != null) {
			buffer.append("FROM ");
			plainSelect.getFromItem().accept(this);
			if(plainSelect.getFromItem().getAlias()!=null){
				buffer.append(" AS \"" + plainSelect.getFromItem().getAlias() + "\" ");
			}
		}

		if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
			for (Iterator iter = plainSelect.getJoins().iterator(); iter.hasNext();) {
				Join join = (Join) iter.next();
				deparseJoin(join);		
			}
		}

		if (plainSelect.getWhere() != null) {
			buffer.append(" WHERE ");
			plainSelect.getWhere().accept(expressionVisitor);
		}

		if (plainSelect.getGroupByColumnReferences() != null) {
			buffer.append(" GROUP BY ");
			for (Iterator iter = plainSelect.getGroupByColumnReferences().iterator(); iter.hasNext();) {
				ColumnReference columnReference = (ColumnReference) iter.next();
				columnReference.accept(this);
				if (iter.hasNext()) {
					buffer.append(", ");
				}
			}
		}

		if (plainSelect.getHaving() != null) {
			buffer.append(" HAVING ");
			plainSelect.getHaving().accept(expressionVisitor);
		}

		if (plainSelect.getOrderByElements() != null) {
			deparseOrderBy(plainSelect.getOrderByElements());
		}

		if (plainSelect.getLimit() != null) {
			deparseLimit(plainSelect.getLimit());
		}

	}
	
	
	@Override
	public void visit(Table tableName) {
		if(tableName.getSchemaName()!=null){
			buffer.append("\"" + tableName.getSchemaName() + "\"");
		}
		buffer.append("\"" + tableName.getName() + "\"");
		
	}
	
	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		selectExpressionItem.getExpression().accept(expressionVisitor);
		if (selectExpressionItem.getAlias() != null) {
			buffer.append(" AS \"" + selectExpressionItem.getAlias()+"\"");
		}

	}
	
	@Override
	public void visit(Column column) {
		if(column.getTable().getAlias()!=null){
			buffer.append("\"" + column.getTable().getAlias() + "\".\"" + column.getColumnName() + "\"");
		}else{
			buffer.append("\"" + column.getColumnName() + "\"");
		}
		
	}
	
	@Override
	public void deparseJoin(Join join) {
		if (join.isSimple())
			buffer.append(" CROSS ");

		else if (join.isRight())
				buffer.append(" RIGHT ");
			else if (join.isNatural())
				buffer.append(" NATURAL ");
			else if (join.isFull())
				buffer.append(" FULL ");
			else if (join.isLeft())
				buffer.append(" LEFT ");
			
			if (join.isOuter())
				buffer.append(" OUTER ");
			else if (join.isInner())
				buffer.append(" INNER ");

			buffer.append("JOIN ");

		
		
		FromItem fromItem = join.getRightItem();
		fromItem.accept(this);
		if (fromItem.getAlias() != null) {
			buffer.append(" AS \"" + fromItem.getAlias()+"\"");
		}
		if (join.getOnExpression() != null) {
			buffer.append(" ON ");
			join.getOnExpression().accept(expressionVisitor);
		}
		if (join.getUsingColumns() != null) {
			buffer.append(" USING ( ");
			for (Iterator iterator = join.getUsingColumns().iterator(); iterator.hasNext();) {
				Column column = (Column) iterator.next();
				buffer.append(column.getWholeColumnName());
				if (iterator.hasNext()) {
					buffer.append(" ,");
				}
			}
			buffer.append(")");
		}

	}
}
