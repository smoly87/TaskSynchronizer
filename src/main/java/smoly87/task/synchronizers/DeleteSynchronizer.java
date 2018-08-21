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

    public DeleteSynchronizer(String table1, String table2) {
        super(table1, table2);
    }

    @Override
    protected Query createQuerySetParams(String query){
        Query queryObj = em.createNativeQuery(query);
        return queryObj;
    }
   
    @Override
    protected String getQueryString() {
         String s = String.join("\n",
               "DELETE from @t1",
               "WHERE @t1.id IN ",
               "(SELECT a.id FROM @t1 as a LEFT  JOIN @t2 as b ON a.id = b.id Where b.id IS null )" //and a.LAST_MODIFIED <= :revision_date
         );
         return s;
    }
    
}
