package org.aksw.sparqlmap.config.syntax;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public abstract class ParsingUtils {
	
	public static SelectBody extractSelectBody(Statement statement){
		
		final List<SelectBody> bodies = new ArrayList<SelectBody>();
		
		statement.accept(new StatementVisitor() {

			@Override
			public void visit(CreateTable arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(Truncate arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(Drop arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(Replace arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(Insert arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(Update arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(Delete arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(Select select) {
				bodies.add(select.getSelectBody());

				

			}
		});
		
		if(bodies.size()>1){
			throw new RuntimeException("More than one Select Body found, please check!");
		}
		
		return bodies.get(0);

	
	}
	
	public static List<SelectItem> extractSelectItems(SelectBody selectBody){
		final List<SelectItem> selectItems = new ArrayList<SelectItem>();
		selectBody.accept(new SelectVisitor() {

			@Override
			public void visit(Union arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(PlainSelect plainSelect) {
				selectItems.addAll(plainSelect.getSelectItems());

			}
		});

		return selectItems;
		
	}
	
	
	
}
