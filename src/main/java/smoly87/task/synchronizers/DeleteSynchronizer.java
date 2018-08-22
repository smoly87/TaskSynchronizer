/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.synchronizers;

import javax.persistence.Query;
/**
 *
 * @author Andrey
 */
public class DeleteSynchronizer extends TableSynchronizer{

    public DeleteSynchronizer(String mainTable, String subordinateTable) {
        super(mainTable, subordinateTable);
    }

    @Override
    protected Query createQuerySetParams(String query){
        Query queryObj = em.createNativeQuery(query);
        return queryObj;
    }
   
    @Override
    protected String getQueryString() {
         String s = String.join("\n",
               "DELETE from @mainTable",
               "WHERE @mainTable.id IN ",
               "(SELECT a.id FROM @mainTable as a LEFT  JOIN @subordinateTable as b ON a.id = b.id Where b.id IS null )"
         );
         return s;
    }
    
}
