package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public interface Wrapper {
	
	SelectBody getSelectBody();
	Set<String> getVarsMentioned();
	List<SelectExpressionItem> getSelectExpressionItems();
	

}
