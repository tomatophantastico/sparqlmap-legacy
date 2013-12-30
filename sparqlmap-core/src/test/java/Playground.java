import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.schema.Table;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.junit.Assert;
import org.junit.Test;

import com.hp.hpl.jena.sparql.core.Var;



public class Playground {
	
	@Test
	public void testVarEquivalence(){
		HashSet<Var> vars = new HashSet<Var>();
		vars.add(Var.alloc("a"));
		vars.add(Var.alloc("a"));
		Assert.assertTrue(vars.size()==2);
		

		
		
	}
	
	
	@Test
	public void testTableEquivalence(){
		
		
		Set<Table> tables =  new HashSet<Table>();
		Table test1 = ColumnHelper.createTable("test");
		tables.add( test1);
		Table test2 = ColumnHelper.createTable("test");
		assertTrue(test1.hashCode()==test2.hashCode());
		assertTrue(tables.contains(test2));
		assertTrue(test1.equals(test2));
	}
	
	
	@Test
	public void testStuff(){
		String a = "a";
	
		

		Set<StringWrapper> set1 = new HashSet<StringWrapper>();
		set1.add(new StringWrapper(a));
		Set<StringWrapper> set2 = new HashSet<Playground.StringWrapper>();
		set2.add(new StringWrapper(a));
		Assert.assertTrue(set1.equals(set2));
		
		
	}
	
	
	private class StringWrapper{
		private String mystring;


		public StringWrapper(String mystring) {
			super();
			this.mystring = mystring;
		}
		
		public String getMystring() {
			return mystring;
		}

		

		private Playground getOuterType() {
			return Playground.this;
		}
		
		
		
	}	
}

