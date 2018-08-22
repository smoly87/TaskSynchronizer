/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.synchronizers;
import static smoly87.task.main.SQLStringUtils.LINE_SEPARATOR;
/**
 *
 * @author Andrey
 */
public class InsertSynchronizer extends TableSynchronizer{

    public InsertSynchronizer(String mainTable, String subordinateTable) {
        super(mainTable, subordinateTable);
    }
   
    @Override
    protected String getQueryString() {
         String s = String.join(LINE_SEPARATOR,
                 "INSERT INTO @subordinateTable(Id,  Name, Description)",
                 "(SELECT  a.Id, a.Name, a.Description FROM  @mainTable as a Left JOIN @subordinateTable as b ON a.id = b.id Where b.id IS null and a.LAST_MODIFIED > :revision_date)"

         );
         return s;
    }
    
}
