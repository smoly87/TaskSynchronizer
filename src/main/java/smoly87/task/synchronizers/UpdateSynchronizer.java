/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.synchronizers;


import static smoly87.task.main.StringUtils.LINE_SEPARATOR;
/**
 *
 * @author Andrey
 */
public class UpdateSynchronizer extends TableSynchronizer{
    
    @Override
    protected String getQueryString() {
         String s = String.join(LINE_SEPARATOR,
                  "UPDATE @subordinateTable a",
                  "SET a.Name = (select b.name from @mainTable b where b.id = a.id),",
                  "a.Description = (select b.Description from @mainTable b where b.id = a.id)",
                  "WHERE EXISTS",
                      "(SELECT * from @mainTable b WHERE b.id = a.id and b.LAST_MODIFIED > :revision_date and b.LAST_MODIFIED > a.LAST_MODIFIED and b.id )");
         return s;
    }
   
    public UpdateSynchronizer( String mainTable, String subordinateTable) {
        super(mainTable, subordinateTable);
  
    }
    
}
