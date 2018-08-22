/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smoly87.task.main;

/**
 *
 * @author Andrey
 */
public class TableSizeChecker {
    protected int mainTableInitSize;
    protected int subordinateTableInitSize;

    public TableSizeChecker(int mainTableInitSize, int subordinateTableInitSize) {
        this.mainTableInitSize = mainTableInitSize;
        this.subordinateTableInitSize = subordinateTableInitSize;
    }
    
    public boolean isDifferenceEqualsAndEqualToDelta(int mainTableFinalSize, int subordinateTableFinalSize, int delta){
        int diffMain = mainTableFinalSize - mainTableInitSize;
        int diffSubordinate = subordinateTableFinalSize - subordinateTableInitSize;
        if (diffMain != diffSubordinate) return false;
        return (diffMain == delta);
    }
}
