package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import com.google.common.collect.LinkedHashMultimap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.query.Mapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpN;

public class MappingUtils {

	public static Map<Node, Set<Triple>> createSBlock(List<Triple> triples) {
		return createSBlock(new HashSet(triples));
	}
	
	public static Map<Node, Set<Triple>> createSBlock(Set<Triple> triples) {
		Map<Node, Set<Triple>> triplesBySubject = new HashMap<Node, Set<Triple>>();

		for (Triple triple : triples) {
			Node subject = triple.getSubject();
			if (!triplesBySubject.containsKey(subject)) {
				triplesBySubject.put(subject, new HashSet<Triple>());
			}
			triplesBySubject.get(subject).add(triple);

		}
		return triplesBySubject;
	}

	private int i = 0;

	private Map<Op, String> aliasMap = new HashMap();

	public String getAliasFor(Op op) {
		String alias = aliasMap.get(op);
		if (alias == null) {
			alias = op.getName() + "_" + i++;
			aliasMap.put(op, alias);
		}
		return alias;
	}

	public static String getNodeString(Node s) {
		if (s.isVariable()) {
			return s.getName();
		} else {
			return s.toString().substring(s.toString().lastIndexOf("/"),
					s.toString().length() + 1);
		}

	}

	public static Function createConcat(Expression... concats) {

		Function concat = new Function();
		concat.setName("CONCAT");

		List<Expression> expressions = new ArrayList<Expression>();
		ExpressionList expList = new ExpressionList();
		expList.setExpressions(expressions);
		expressions.add(concats[0]);
		if (concats.length > 2) {
			expressions.add(createConcat(Arrays.copyOfRange(concats, 1,
					concats.length)));

		} else {
			expressions.add(concats[1]);
		}
		concat.setParameters(expList);

		return concat;
	}

	public static void addCols(PlainSelect select, Expression col,
			String alias, Mapping map, ColumDefinition coldef) {

		if (map.getIdColumn().getColname().equals(coldef.getColname())) {
			addCol(select,
					createConcat(
							col,
							new StringValue("'" + map.getIdColumn().getLinkedDataPath() + "'")),
					alias + "_iri");
		}

		addCol(select, col, alias);
		addCol(select, col, alias + "_txt");
		addCol(select, col, alias + "_int");
		addCol(select, col, alias + "_dbl");

	}

	public static void addCol(PlainSelect select, Expression col, String alias) {
		if (select.getSelectItems() == null) {
			select.setSelectItems(new ArrayList<SelectExpressionItem>());
		}

		SelectExpressionItem s_sei = new SelectExpressionItem();
		s_sei.setAlias(alias);
		s_sei.setExpression(col);

		// check if the sei is already contained in the select expression. we
		// have to go deep into it, as equals is not implemented
		for (Object obj : select.getSelectItems()) {
			SelectExpressionItem sei = (SelectExpressionItem) obj;
			if (sei.toString().equals(s_sei.toString())) {
				return;
			}

		}

		select.getSelectItems().add(s_sei);

	}

	public static void addCol(PlainSelect select, String tablename,
			String colname, String alias) {

		Column col = new Column();
		col.setColumnName(colname);
		Table tab = new Table();
		tab.setName(tablename);
		tab.setAlias(tablename);
		col.setTable(tab);

		addCol(select, col, alias);
	}

	public static Column createCol(String tablename, String colname) {
		Column col = new Column();
		col.setColumnName(colname);
		Table tab = new Table();
		tab.setName(tablename);
		tab.setAlias(tablename);
		col.setTable(tab);

		return col;

	}

	public static EqualsTo createUriFilter(Column col, Node uri,
			MappingConfiguration mconf) {

		EqualsTo eq = new EqualsTo();
		eq.setLeftExpression(col);
		String rightId = mconf.getIdForInstanceUri(uri.getURI());
		if (rightId != null) {

			eq.setRightExpression(new StringValue(rightId));

		} else {

			throw new ImplementationException(
					"Non-Mappable URI encountered in mapping.");
		}

		return eq;

	}
	
	
	

	public static class OpTree {

		public OpTree(Op root) {
			op = root;
			analyzeFilters(op);
		}

		Map<Op, Op> parentOfOp = new HashMap<Op, Op>();
		LinkedHashMultimap<Op, Op> siblingsOfOp = LinkedHashMultimap.create();
		Op op;
		
		
		Set<Op> getVisiblefor(Op op) {
			Set<Op> ops = new HashSet<Op>();
			ops.addAll(getOpsDownFrom(op));
			ops.addAll(getOpsUpFrom(op));
			
			
			return ops;
		}
		
		
		// check the siblings for filters
		// considering down: the root of the query is on top.
		private Collection<Op> getOpsDownFrom(Op here){
			Collection<Op> ops = new HashSet<Op>();
	
			
			ops.add(here);
			for (Op sibOp : siblingsOfOp.get(here)) {
				ops.addAll(getOpsDownFrom(sibOp));
			}
			
			return ops;
		}
		
		// check the parents for filters
			private Collection<Op> getOpsUpFrom(Op here){
				Collection<Op> ops = new HashSet<Op>();
				ops.add(here);
				
				Op parent = parentOfOp.get(here);
				if(parent !=null){
					
					
				//we branch down in certain conditions
					
					
				if(here instanceof OpLeftJoin){
					
					// if we are going but find ourselves on the right side of a a left (optional) join, we add the left side
					OpLeftJoin opleft = (OpLeftJoin) here;
					ops.addAll(getOpsDownFrom(opleft.getLeft()));
				}
					
				ops.addAll(getOpsUpFrom(parent));
				}
				
				
				return ops;
			}
		
		
		
		// filling the tables
		private void analyzeFilters(Op op) {

			// create the tree here
			if (op instanceof Op1) {
				Op1 op1 = (Op1) op;

				parentOfOp.put(op1.getSubOp(), op1);
				siblingsOfOp.put(op1, op1.getSubOp());
				analyzeFilters(op1.getSubOp());

			} else if (op instanceof Op2) {
				Op2 op2 = (Op2) op;
				parentOfOp.put(op2.getLeft(), op2);
				parentOfOp.put(op2.getRight(), op2);
				siblingsOfOp.put(op2, op2.getLeft());
				siblingsOfOp.put(op2, op2.getRight());
				analyzeFilters(op2.getLeft());
				analyzeFilters(op2.getRight());

			} else if (op instanceof Op0) {
				// do nothing here

			} else if (op instanceof OpN) {
				OpN opn = (OpN) op;
				for (Op opnSibling : opn.getElements()) {
					parentOfOp.put(opnSibling, opn);
					siblingsOfOp.put(opn, opnSibling);
					analyzeFilters(opnSibling);
				}

			}

		}

	}
}
