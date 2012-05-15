package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import static org.junit.Assert.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

import org.junit.Test;

public class FilterUtilTest {

	@Test
	public void testShortCut() {
		
		
		EqualsTo eq = new EqualsTo();
		
		StringValue left = new StringValue("'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor244/Offer480636'");
		String leftString = left.toString();
		
 
		Expression right = FilterUtil.concat(new StringValue("'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer'"),
		FilterUtil.createColumn( "product_isValueOf","producer"),new StringValue( "'/Product'"), FilterUtil.createColumn( "product_isValueOf", "nr"));
		String rightString = right.toString();
		
		eq.setLeftExpression(left);
		eq.setRightExpression(right);
		
		Expression neweq = FilterUtil.shortCut(eq);
		
		assertTrue(neweq instanceof EqualsTo);
		eq = (EqualsTo) neweq;
		
		assertTrue(leftString.equals(eq.getLeftExpression().toString()));
		assertTrue(rightString.equals(eq.getRightExpression().toString()));
		
		
		
		
	}

}
