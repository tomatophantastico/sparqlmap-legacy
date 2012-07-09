import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;



public class Playground {
	
	
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

