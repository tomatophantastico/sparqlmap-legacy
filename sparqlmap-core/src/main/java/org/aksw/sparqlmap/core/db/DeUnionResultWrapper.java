package org.aksw.sparqlmap.core.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.ResultBinding;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;

public class DeUnionResultWrapper implements ResultSet{
	
	// maps the "real" variables to the set of variables that contain the deunified values.
//	Multimap<String, String> var2duvar = LinkedHashMultimap.create();
//	Map<String,String> duvar2var = new HashMap<String, String>();
	Multimap<String,String> suffix2duvar = TreeMultimap.create();
	List<String> non_du_var = new ArrayList<String>();
	List<String> vars = new ArrayList<String>();
	
	
	SQLResultSetWrapper srs;
	List<Binding> duSolutions = new ArrayList<Binding>();
	
	int count = 0;
	//int maxDu=0;
	
	public DeUnionResultWrapper(SQLResultSetWrapper srs) {
		super();
		this.srs = srs;
		List<String> oVars = new ArrayList<String>(srs.getResultVars());
//		Collections.sort(oVars);
		for(String ovar: oVars){
			if(ovar.matches(".*-du[0-9][0-9]$")){
				//put it in the du map
				//var2duvar.put(ovar.substring(0,ovar.length()-5), ovar);
				//duvar2var.put(ovar, ovar.substring(0,ovar.length()-5));
				suffix2duvar.put( ovar.substring(ovar.length()-2), ovar);
				vars.add(ovar.substring(0,ovar.length()-5));
//				Integer duNum = Integer.parseInt();
//				if(duNum>maxDu){
//					maxDu = duNum;
//				}
				
				
			}else{
				non_du_var.add(ovar);
				vars.add(ovar);
			}
		}
		
		
		
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove not implemented");
		
	}


	@Override
	public boolean hasNext() {
		
		return  srs.hasNext()||!duSolutions.isEmpty();
	}

	@Override
	public QuerySolution next() {
		return nextSolution();
	}


	@Override
	public QuerySolution nextSolution() {
		return new ResultBinding(null, nextBinding());
	}

	@Override
	public Binding nextBinding() {
		count++;
		//first check the precomputed part
		if(!duSolutions.isEmpty()){
			return duSolutions.remove(0);

		}else{
			//nothing precomputed, so we check the wrapped resultset
			
			Binding binding = this.srs.nextBinding();
			
			//are there any deunification-variables in there?
			Iterator<Var> bindingVarI = binding.vars();
			
			boolean isDuBinding = false;
			if(!suffix2duvar.isEmpty()){
				while(bindingVarI.hasNext()){
					Var bindingVar = bindingVarI.next();
					if(suffix2duvar.containsValue(bindingVar.getName())){
						isDuBinding = true;
						break;
					}
				}
			}
			
			if(isDuBinding){
				//check if there is a non -du binding
				BindingMap nDuBind = BindingFactory.create();
				for(String nDuVar: non_du_var){
					Var nDu = Var.alloc(nDuVar);
					if(binding.get(nDu)!=null){
						nDuBind.add(nDu,binding.get(nDu));
					}
				}
				
				if (!nDuBind.isEmpty()){
					this.duSolutions.add(nDuBind);
				}
				
				//add for each du binding a new binding
				for(String suffix: suffix2duvar.keySet() ){
					BindingMap duBind = BindingFactory.create();
					for(String fullDuname: suffix2duvar.get(suffix)){
						Var du = Var.alloc(fullDuname);
						Var realVar = Var.alloc(fullDuname.substring(0,fullDuname.length()-5));
						if(binding.get(du)!=null){
							duBind.add(realVar,binding.get(du));
						}
					}
					if (!duBind.isEmpty()){
						this.duSolutions.add(duBind);
					}
					
				}
				//and return the first
				return duSolutions.remove(0);
			}else{
				// nothing to process, just return the binding
				return binding;
				
			}
		}
		
		
	}

	@Override
	public int getRowNumber() {
		return count;
	}

	@Override
	public List<String> getResultVars() {
		return new ArrayList<String>(vars);
	}

	@Override
	public Model getResourceModel() {
		// TODO Auto-generated method stub
		return null;
	}
	

	
	

}
