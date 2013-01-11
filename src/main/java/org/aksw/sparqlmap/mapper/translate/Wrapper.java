package org.aksw.sparqlmap.mapper.translate;

import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public interface Wrapper {
	
	SelectBody getSelectBody();
	Set<String> getVarsMentioned();
	List<SelectItem> getSelectExpressionItems();
	

}
