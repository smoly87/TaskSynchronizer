/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.synchronizers;


import java.util.LinkedList;
import java.util.List;
import javax.persistence.Query;

/**
 *
 * @author Andrey
 */
public class UpdateSynchronizer extends TableSynchronizer{
    protected List<Integer> excludedIdsList;
    @Override
    protected String getQueryString() {
         String s = String.join("\n"
                , "UPDATE @t2 a",
                  "SET a.Name = (select b.name from @t1 b where b.id = a.id),",
                  "a.Description = (select b.Description from @t1 b where b.id = a.id)",
                  "WHERE EXISTS",
                      "(SELECT * from @t1 b WHERE b.id = a.id and b.LAST_MODIFIED > :revision_date and b.LAST_MODIFIED > a.LAST_MODIFIED and b.id NOT IN(:excludedIds))");
         return s;
    }
    
    protected List<Integer>  getExcludedIds(String mainTable, String subordinateTable){
        String query = "select b.id from @t1 b where b.LAST_MODIFIED > :revision_date  and b.id NOT IN(:excludedIds)";
               query = processQuery(query, mainTable, subordinateTable);
       
        return createQuerySetParams(query).getResultList();
    }

    @Override
    protected Query createQuerySetParams(String query){
        Query queryObj = super.createQuerySetParams(query)
                              .setParameter("excludedIds", excludedIdsList);
        return queryObj;
    }

    @Override
    public void synchronisation(){

        List<Integer> curExcludedIds = getExcludedIds(table1, table2);
        executeQuery(table1, table2);
        excludedIdsList = curExcludedIds;
        executeQuery(table2, table1);
        
    }
    
    public UpdateSynchronizer( String table1, String table2) {
        super(table1, table2);
        excludedIdsList = new LinkedList<>();
    }
    
}
