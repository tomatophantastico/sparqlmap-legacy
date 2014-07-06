package org.aksw.sparqlmap;

import java.io.File;

import org.junit.Test;

public class AggregateFunctionsTest extends BSBMTestOnHSQL {
  
  
  @Test
  public void numberOfProd(){
    String query = "SELECT (COUNT(?prod) AS ?no) { ?prod <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?propNum1  }";
    executeAndCompareSelect(query, new File("./src/test/resources/arregates/numberOfProd.qres"));
  }

  @Test
  public void numberOfProdPerNumProperty(){
    String query = "SELECT ?propNum1 (COUNT(?prod) AS ?no) { ?prod <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?propNum1  }";
    executeAndCompareSelect(query, new File("./src/test/resources/arregates/numberOfProd.qres"));
  }
  
  @Test
  public void numberOfProdPerNumPropertyWithGroupBy(){
    String query = "SELECT ?propNum1 (COUNT(?prod) AS ?no) { ?prod <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?propNum1  } GROUP BY ?propNum1";
    executeAndCompareSelect(query, new File("./src/test/resources/arregates/numberOfProd.qres"));
  }
  
  @Test
  public void numberOfProdPerNumPropertyWithGroupByAndHaving(){
    String query = "SELECT ?propNum1 (COUNT(?prod) AS ?no) { ?prod <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?propNum1  } GROUP BY ?propNum1 HAVING(COUNT(?prod)>10)";
    executeAndCompareSelect(query, new File("./src/test/resources/arregates/numberOfProd.qres"));
  }
  

  @Override
  public String getTestName() {
    
    return "AggregateFunctionTest";
  }

}
