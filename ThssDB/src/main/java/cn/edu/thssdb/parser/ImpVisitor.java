package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.SchemaLengthMismatchException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static cn.edu.thssdb.schema.Column.parseEntry;
import static cn.edu.thssdb.type.ColumnType.*;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 *     K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 *         K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 *
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 *
 * We give you a few examples to convert the tree, including create/drop/quit.
 * You need to finish the codes for parsing the other rooted trees marked TODO.
 */

public class ImpVisitor extends SQLBaseVisitor<Object> {
    private Manager manager;
    private long session;

    public ImpVisitor(Manager manager, long session) {
        super();
        this.manager = manager;
        this.session = session;
    }

    private Database GetCurrentDB() {
        Database currentDB = manager.getCurrentDatabase();
        if(currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public Cell createCell(ColumnType columnType,String compareValue){
        Cell value=null;
        switch (columnType){
            case STRING:
                value=new Cell(compareValue);
                break;
            case INT:
                value=new Cell(Integer.parseInt(compareValue));
                break;
            case LONG:
                value=new Cell(Long.parseLong(compareValue));
                break;
            case FLOAT:
                value=new Cell(Float.parseFloat(compareValue));
                break;
            case DOUBLE:
                value=new Cell(Double.parseDouble(compareValue));
                break;
            default:
                break;
        }
        return value;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null)  return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        if (ctx.show_meta_stmt() != null) return new QueryResult(visitShow_meta_stmt(ctx.show_meta_stmt()));
        return null;
    }

    /**
     创建数据库
     */
    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        try {
            manager.createDatabaseIfNotExists(ctx.database_name().getText().toLowerCase());
            manager.persist();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create database " + ctx.database_name().getText() + ".";
    }

    /**
     删除数据库
     */
    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            manager.deleteDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop database " + ctx.database_name().getText() + ".";
    }

    /**
     切换数据库
     */
    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        try {
            manager.switchDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Switch to database " + ctx.database_name().getText() + ".";
    }

    /**
     删除表格
     */
    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        try {
            GetCurrentDB().drop(ctx.table_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop table " + ctx.table_name().getText() + ".";
    }

    // 助教说要这俩函数
    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }

    /**
     * TODO
     创建表格
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText().toLowerCase();
            List<SQLParser.Column_defContext> columnDefItems = ctx.column_def();
            Column[] columns = new Column[columnDefItems.size()];

            for (int i = 0; i < columnDefItems.size(); ++i){
                SQLParser.Column_defContext columnDefItem =  ctx.column_def(i);
                // 获取属性名
                String columnName = columnDefItem.column_name().getText().toLowerCase();

                // 获取属性类别与最大长度
                ColumnType columnType = null;
                int columnMaxLength = 0;
                String columnTypeName = columnDefItem.type_name().getText().toLowerCase();
                switch (columnTypeName){
                    case "int":
                        columnType = INT;
                        break;
                    case "long":
                        columnType = LONG;
                        break;
                    case "float":
                        columnType = FLOAT;
                        break;
                    case "double":
                        columnType = DOUBLE;
                        break;
                    default:
                        if (columnTypeName.substring(0,6).equals("string")){
                            columnType = STRING;
                            columnMaxLength = Integer.parseInt(columnTypeName.substring(7, columnTypeName.length() - 1));
                        }
                        break;
                }

                // 获取属性约束，这里是每个属性自身的约束
                int columnPrimaryKey = 0;
                Boolean columnNotNull = false;
                for (int j = 0; j < columnDefItem.column_constraint().size(); ++j){
                    String columnConstraint = columnDefItem.column_constraint(j).getText().toLowerCase();
                    switch (columnConstraint){
                        case "notnull":
                            columnNotNull = true;
                            break;
                        case "primarykey":
                            columnPrimaryKey = 1;
                            columnNotNull = true; // 主码必须非空
                            break;
                    }
                }
                Column column = new Column( columnName, columnType, columnPrimaryKey, columnNotNull, columnMaxLength);
                columns[i] = column;
            }

            // 获取属性约束，这里是整个表的约束
            System.out.println();
            if(ctx.table_constraint() != null){
                // 对于每条约束语句
                for (int j = 0; j < ctx.table_constraint().column_name().size(); ++j){
                    String columnPrimaryName = ctx.table_constraint().column_name(j).getText().toLowerCase();
                    System.out.println(columnPrimaryName);

                    // 对于每条约束语句的主键进行修改
                    for (int i = 0; i < columnDefItems.size(); ++i){
                        if (columns[i].getColumnName().equals(columnPrimaryName)){
                            columns[i].setPrimary(1);
                            columns[i].setNotNull(true);
                        }
                    }
                }
            }
            GetCurrentDB().create(tableName, columns);
            return "Create table " + tableName + ".";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText().toLowerCase();
            Table table = GetCurrentDB().get(tableName);
            if (table == null){
                throw new TableNotExistException();
            }

            List<SQLParser.Column_nameContext> columnName = ctx.column_name();
            List<SQLParser.Value_entryContext> valueEntries = ctx.value_entry();
            ArrayList<Integer> columnIndex = new ArrayList<>();
            if (columnName.size() == 0){
                for (int i = 0; i < table.columns.size(); ++i){
                    columnIndex.add(i);
                }
            }
            else{
                for (int i = 0; i < columnName.size(); ++i){
                    int index = table.searchColumn(ctx.column_name(i).getText().toLowerCase());
                    if (index < 0){
                        throw new KeyNotExistException();
                    }
                    columnIndex.add(index);
                }
            }
            for (int j = 0; j < valueEntries.size(); ++j){
                SQLParser.Value_entryContext valueEntry = ctx.value_entry(j);
                if (valueEntry.literal_value().size() != columnIndex.size()){
                    throw new SchemaLengthMismatchException(columnIndex.size(), valueEntry.literal_value().size(), " during insert");
                }
                Cell[] cells = new Cell[table.columns.size()];
                for (int k = 0; k < columnIndex.size(); ++k){
                    cells[columnIndex.get(k)] = parseEntry(valueEntry.literal_value(k).getText(), table.columns.get(columnIndex.get(k)));
                }
                table.insert(new Row(cells));
            }

            return "Insert into table " + tableName + ".";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        Table table = GetCurrentDB().get(tableName);
        if (table == null){
            throw new TableNotExistException();
        }
//        String tableName=ctx.table_name().getText().toLowerCase();
//        Table table=manager.currentDatabase.get(tableName);
//        if(table==null)return "database doesn't exist.";
        String retString="delete "+tableName;

        String columnName = ctx.multiple_condition().condition().expression(0).comparer().column_full_name().column_name().getText().toLowerCase();
        String compareValue = ctx.multiple_condition().condition().expression(1).comparer().literal_value().getText();
        SQLParser.ComparatorContext comparator = ctx.multiple_condition().condition().comparator();
        int columnIndex = table.searchColumn(columnName);
        if (columnIndex < 0) {
            throw new KeyNotExistException();
        }
        Cell value = parseEntry(compareValue, table.columns.get(columnIndex));
//        if(value==null)return "type doesn't exist.";
//        Cell value = null;
//        ColumnType columnType=null;
//        for (int i=0;i<table.columns.size();i++) {
//            Column x=table.columns.get(i);
//            if(x.getColumnName().equals(columnName)){
//                columnType=x.getColumnType();
//                columnIndex=i;
//                break;
//            }
//        }
//        if(columnType==null)return "column doesn't exist.";
//        value=createCell(columnType,compareValue);
//        if(value==null)return "type doesn't exist.";
        ArrayList<Row>toDelete=new ArrayList<>();
        Iterator<Row> iterator=table.iterator();
        if(ctx.multiple_condition().condition().comparator().EQ()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(columnIndex).compareTo(value)==0) toDelete.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().NE()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(columnIndex).compareTo(value)!=0) toDelete.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().LE()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(columnIndex).compareTo(value)<=0) toDelete.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().GE()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(columnIndex).compareTo(value)>=0) toDelete.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().LT()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(columnIndex).compareTo(value)<0) toDelete.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().GT()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(columnIndex).compareTo(value)>0) toDelete.add(row);
            }
        }else {
            return "operator doesn't exist";
        }
        for (Row x:toDelete) {
            table.delete(x);
        }
        return retString;
    }

    /**
     * TODO
     表格项更新
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        Table table = GetCurrentDB().get(tableName);
        if (table == null){
            throw new TableNotExistException();
        }
//        String tableName=ctx.table_name().getText().toLowerCase();
//        Table table=manager.currentDatabase.get(tableName);
//        if(table==null)return "database doesn't exist.";
        String retString="update "+tableName;

        String expressionColumnName=ctx.column_name().getText().toLowerCase();
        String expressionUpdateValue=ctx.expression().comparer().literal_value().getText();
        String conditionColumnName = ctx.multiple_condition().condition().expression(0).comparer().column_full_name().column_name().getText().toLowerCase();
        String conditionCompareValue = ctx.multiple_condition().condition().expression(1).comparer().literal_value().getText();
        SQLParser.ComparatorContext comparator = ctx.multiple_condition().condition().comparator();
        int primaryIndex=table.getPrimaryIndex();

        int expressionColumnIndex = table.searchColumn(expressionColumnName);
        if (expressionColumnIndex < 0){
            throw new KeyNotExistException();
        }
        int conditionColumnIndex = table.searchColumn(conditionColumnName);
        if (conditionColumnIndex < 0){
            throw new KeyNotExistException();
        }
        Cell expressionValue=parseEntry(expressionUpdateValue, table.columns.get(expressionColumnIndex));
        Cell conditionValue=parseEntry(conditionCompareValue, table.columns.get(conditionColumnIndex));
//        int expressionColumnIndex=0,conditionColumnIndex=0,primaryIndex=table.getPrimaryIndex();
//        Cell expressionValue = null,conditionValue=null;
//        ColumnType expressionColumnType=null,conditionColumnType=null;
//        for (int i=0;i<table.columns.size();i++) {
//            Column x=table.columns.get(i);
//            if(x.getColumnName().equals(expressionColumnName)){
//                expressionColumnType=x.getColumnType();
//                expressionColumnIndex=i;
//            }
//            if(x.getColumnName().equals(conditionColumnName)){
//                conditionColumnType=x.getColumnType();
//                conditionColumnIndex=i;
//            }
//        }
//        if(expressionColumnType==null)return "column doesn't exist.";
//        if(conditionColumnType==null)return "column doesn't exist.";
//        expressionValue=createCell(expressionColumnType,expressionUpdateValue);
//        conditionValue=createCell(conditionColumnType,conditionCompareValue);
//        if(expressionValue==null)return "type doesn't exist.";
//        if(conditionValue==null)return "type doesn't exist.";
        ArrayList<Row>toUpdate=new ArrayList<>();
        Iterator<Row> iterator=table.iterator();
        if(ctx.multiple_condition().condition().comparator().EQ()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(conditionColumnIndex).compareTo(conditionValue)==0) toUpdate.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().NE()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(conditionColumnIndex).compareTo(conditionValue)!=0) toUpdate.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().LE()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(conditionColumnIndex).compareTo(conditionValue)<=0) toUpdate.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().GE()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(conditionColumnIndex).compareTo(conditionValue)>=0) toUpdate.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().LT()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(conditionColumnIndex).compareTo(conditionValue)<0) toUpdate.add(row);
            }
        }else if(ctx.multiple_condition().condition().comparator().GT()!=null) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (row.getEntries().get(conditionColumnIndex).compareTo(conditionValue)>0) toUpdate.add(row);
            }
        }else {
            return "operator doesn't exist";
        }
        for (Row x:toUpdate) {
            table.update(x.getEntries().get(primaryIndex), x.newUpdateRow(expressionColumnIndex,expressionValue));
        }
        return retString;
    }

    /**
     * TODO
     表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        ArrayList<Column>table1ColumnName=new ArrayList<>(),table2ColumnName=new ArrayList<>();
        ArrayList<Integer>table1ColumnIndex=new ArrayList<>(),table2ColumnIndex=new ArrayList<>();
        ArrayList<Row>table1Row=new ArrayList<>(),table2Row=new ArrayList<>();
        String table1Name=ctx.table_query(0).table_name(0).getText();
        String table2Name=null;
        Table table1=manager.currentDatabase.get(table1Name);
        if(ctx.table_query(0).K_ON()==null){
            if(ctx.K_WHERE()!=null){
                List<String>columns=new ArrayList<>();
                List<Integer>columnIndex=new ArrayList<>();
                for(int i=0;i<ctx.result_column().size();i++){
                    columns.add(ctx.result_column(i).column_full_name().column_name().getText());
                    columnIndex.add(table1.searchColumn(ctx.result_column(i).column_full_name().column_name().getText()));
                }
                String conditionColumnName = ctx.multiple_condition().condition().expression(0).comparer().column_full_name().column_name().getText().toLowerCase();
                String conditionCompareValue = ctx.multiple_condition().condition().expression(1).comparer().literal_value().getText();
                SQLParser.ComparatorContext comparator = ctx.multiple_condition().condition().comparator();
                int conditionIndex=table1.searchColumn(conditionColumnName);
                ColumnType conditionType=table1.columns.get(conditionIndex).getColumnType();
                Cell conditionValue=createCell(conditionType,conditionCompareValue);
                Iterator<Row> iterator=table1.iterator();
                if(ctx.multiple_condition().condition().comparator().EQ()!=null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(conditionIndex).compareTo(conditionValue)==0) {
                            ArrayList<Cell> newRow=new ArrayList<>();
                            for(int i=0;i<columnIndex.size();i++){
                                newRow.add(row.getEntries().get(columnIndex.get(i)));
                            }
                            table1Row.add(new Row(newRow));
                        }
                    }
                }else if(ctx.multiple_condition().condition().comparator().NE()!=null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(conditionIndex).compareTo(conditionValue)!=0) {
                            ArrayList<Cell> newRow=new ArrayList<>();
                            for(int i=0;i<columnIndex.size();i++){
                                newRow.add(row.getEntries().get(columnIndex.get(i)));
                            }
                            table1Row.add(new Row(newRow));
                        }
                    }
                }else if(ctx.multiple_condition().condition().comparator().LE()!=null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(conditionIndex).compareTo(conditionValue)<=0) {
                            ArrayList<Cell> newRow=new ArrayList<>();
                            for(int i=0;i<columnIndex.size();i++){
                                newRow.add(row.getEntries().get(columnIndex.get(i)));
                            }
                            table1Row.add(new Row(newRow));
                        }
                    }
                }else if(ctx.multiple_condition().condition().comparator().GE()!=null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(conditionIndex).compareTo(conditionValue)>=0) {
                            ArrayList<Cell> newRow=new ArrayList<>();
                            for(int i=0;i<columnIndex.size();i++){
                                newRow.add(row.getEntries().get(columnIndex.get(i)));
                            }
                            table1Row.add(new Row(newRow));
                        }
                    }
                }else if(ctx.multiple_condition().condition().comparator().LT()!=null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(conditionIndex).compareTo(conditionValue)<0) {
                            ArrayList<Cell> newRow=new ArrayList<>();
                            for(int i=0;i<columnIndex.size();i++){
                                newRow.add(row.getEntries().get(columnIndex.get(i)));
                            }
                            table1Row.add(new Row(newRow));
                        }
                    }
                }else if(ctx.multiple_condition().condition().comparator().GT()!=null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(conditionIndex).compareTo(conditionValue)>0) {
                            ArrayList<Cell> newRow=new ArrayList<>();
                            for(int i=0;i<columnIndex.size();i++){
                                newRow.add(row.getEntries().get(columnIndex.get(i)));
                            }
                            table1Row.add(new Row(newRow));
                        }
                    }
                }
                return new QueryResult(new QueryTable[] {new QueryTable(columns,table1Row)});
            }else {
                List<String>columns=new ArrayList<>();
                List<Integer>columnIndex=new ArrayList<>();
                for(int i=0;i<ctx.result_column().size();i++){
                    columns.add(ctx.result_column(i).column_full_name().column_name().getText());
                    columnIndex.add(table1.searchColumn(ctx.result_column(i).column_full_name().column_name().getText()));
                }
                Iterator<Row> iterator=table1.iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    ArrayList<Cell> newRow=new ArrayList<>();
                    for(int i=0;i<columnIndex.size();i++){
                        newRow.add(row.getEntries().get(columnIndex.get(i)));
                    }
                    table1Row.add(new Row(newRow));
                }

                QueryResult ret=new QueryResult(new QueryTable[]{new QueryTable(columns,table1Row)});

                return new QueryResult(new QueryTable[] {new QueryTable(columns,table1Row)});
            }
        }else {//ON
            table2Name=ctx.table_query(0).table_name(1).getText();
            Table table2=manager.currentDatabase.get(table2Name);

            String table1ConditionColumn=ctx.table_query(0).multiple_condition().condition().expression(0).comparer().column_full_name().column_name().getText();
            String table2ConditionColumn=ctx.table_query(0).multiple_condition().condition().expression(1).comparer().column_full_name().column_name().getText();
            int table1ConditionIndex=table1.searchColumn(table1ConditionColumn);
            int table2ConditionIndex=table2.searchColumn(table2ConditionColumn);
            SQLParser.ComparatorContext tableComparator=ctx.table_query(0).multiple_condition().condition().comparator();

            if(ctx.K_WHERE()!=null){
                String conditionTableName=ctx.multiple_condition().condition().expression(0).comparer().column_full_name().table_name().getText().toLowerCase();
                String conditionColumnName = ctx.multiple_condition().condition().expression(0).comparer().column_full_name().column_name().getText().toLowerCase();
                String conditionCompareValue = ctx.multiple_condition().condition().expression(1).comparer().literal_value().getText();
                SQLParser.ComparatorContext comparator = ctx.multiple_condition().condition().comparator();
                if(conditionTableName==table1Name){
                    int conditionIndex=table1.searchColumn(conditionColumnName);
                    ColumnType conditionType=table1.columns.get(conditionIndex).getColumnType();
                    Cell conditionValue=createCell(conditionType,conditionCompareValue);
                    Iterator<Row> iterator=table1.iterator();
                    if(ctx.multiple_condition().condition().comparator().EQ()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)==0)table1Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().NE()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)!=0)table1Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().LE()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)<=0)table1Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().GE()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)>=0)table1Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().LT()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)<0)table1Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().GT()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)>0)table1Row.add(row);
                        }
                    }
                    Iterator<Row>iterator2=table2.iterator();
                    while(iterator2.hasNext()){
                        table2Row.add(iterator2.next());
                    }

                }else {
                    int conditionIndex=table2.searchColumn(conditionColumnName);
                    ColumnType conditionType=table2.columns.get(conditionIndex).getColumnType();
                    Cell conditionValue=createCell(conditionType,conditionCompareValue);
                    Iterator<Row> iterator=table2.iterator();
                    if(ctx.multiple_condition().condition().comparator().EQ()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)==0)table2Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().NE()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)!=0)table2Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().LE()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)<=0)table2Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().GE()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)>=0)table2Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().LT()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)<0)table2Row.add(row);
                        }
                    }else if(ctx.multiple_condition().condition().comparator().GT()!=null) {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.getEntries().get(conditionIndex).compareTo(conditionValue)>0)table2Row.add(row);
                        }
                    }
                    Iterator<Row>iterator1=table1.iterator();
                    while(iterator1.hasNext()){
                        table1Row.add(iterator1.next());
                    }
                }
            }else {
                Iterator<Row>iterator1=table1.iterator();
                while(iterator1.hasNext()){
                    table1Row.add(iterator1.next());
                }
                Iterator<Row>iterator2=table2.iterator();
                while(iterator2.hasNext()){
                    table2Row.add(iterator2.next());
                }
            }

            //ON

            List<String>columns=new ArrayList<>();
            List<Row>rows=new ArrayList<>();
            ArrayList<Integer>whichTable=new ArrayList<>(),index=new ArrayList<>();
            for(int i=0;i<ctx.result_column().size();i++){
                String tableName=ctx.result_column(i).column_full_name().table_name().getText();
                String columnName=ctx.result_column(i).column_full_name().column_name().getText();
                columns.add(tableName+'.'+columnName);
                if(tableName.equals(table1Name)){
                    whichTable.add(1);
                    index.add(table1.searchColumn(columnName));
                } else if(tableName.equals(table2Name)) {
                    whichTable.add(2);
                    index.add(table2.searchColumn(columnName));
                } else{
                    throw new KeyNotExistException();
                }
            }


            if(tableComparator.EQ()!=null) {
                for(int i=0;i<table1Row.size();i++) {
                    for (int j = 0; j < table2Row.size(); j++) {
                        if (table1Row.get(i).getEntries().get(table1ConditionIndex).compareTo(table2Row.get(j).getEntries().get(table2ConditionIndex))==0) {
                            ArrayList<Cell>newRow=new ArrayList<>();
                            for(int k=0;k<whichTable.size();k++){
                                if(whichTable.get(k)==1){
                                    newRow.add(table1Row.get(i).getEntries().get(index.get(k)));
                                }else {
                                    newRow.add(table2Row.get(j).getEntries().get(index.get(k)));
                                }
                            }
                            rows.add(new Row(newRow));
                        }
                    }
                }
            }else if(tableComparator.NE()!=null) {
                for(int i=0;i<table1Row.size();i++) {
                    for (int j = 0; j < table2Row.size(); j++) {
                        if (table1Row.get(i).getEntries().get(table1ConditionIndex).compareTo(table2Row.get(j).getEntries().get(table2ConditionIndex))!=0) {
                            ArrayList<Cell>newRow=new ArrayList<>();
                            for(int k=0;k<whichTable.size();k++){
                                if(whichTable.get(k)==1){
                                    newRow.add(table1Row.get(i).getEntries().get(index.get(k)));
                                }else {
                                    newRow.add(table2Row.get(j).getEntries().get(index.get(k)));
                                }
                            }
                            rows.add(new Row(newRow));
                        }
                    }
                }
            }else if(tableComparator.LE()!=null) {
                for(int i=0;i<table1Row.size();i++) {
                    for (int j = 0; j < table2Row.size(); j++) {
                        if (table1Row.get(i).getEntries().get(table1ConditionIndex).compareTo(table2Row.get(j).getEntries().get(table2ConditionIndex))<=0) {
                            ArrayList<Cell>newRow=new ArrayList<>();
                            for(int k=0;k<whichTable.size();k++){
                                if(whichTable.get(k)==1){
                                    newRow.add(table1Row.get(i).getEntries().get(index.get(k)));
                                }else {
                                    newRow.add(table2Row.get(j).getEntries().get(index.get(k)));
                                }
                            }
                            rows.add(new Row(newRow));
                        }
                    }
                }
            }else if(tableComparator.GE()!=null) {
                for(int i=0;i<table1Row.size();i++) {
                    for (int j = 0; j < table2Row.size(); j++) {
                        if (table1Row.get(i).getEntries().get(table1ConditionIndex).compareTo(table2Row.get(j).getEntries().get(table2ConditionIndex))>=0) {
                            ArrayList<Cell>newRow=new ArrayList<>();
                            for(int k=0;k<whichTable.size();k++){
                                if(whichTable.get(k)==1){
                                    newRow.add(table1Row.get(i).getEntries().get(index.get(k)));
                                }else {
                                    newRow.add(table2Row.get(j).getEntries().get(index.get(k)));
                                }
                            }
                            rows.add(new Row(newRow));
                        }
                    }
                }
            }else if(tableComparator.LT()!=null) {
                for(int i=0;i<table1Row.size();i++) {
                    for (int j = 0; j < table2Row.size(); j++) {
                        if (table1Row.get(i).getEntries().get(table1ConditionIndex).compareTo(table2Row.get(j).getEntries().get(table2ConditionIndex))<0) {
                            ArrayList<Cell>newRow=new ArrayList<>();
                            for(int k=0;k<whichTable.size();k++){
                                if(whichTable.get(k)==1){
                                    newRow.add(table1Row.get(i).getEntries().get(index.get(k)));
                                }else {
                                    newRow.add(table2Row.get(j).getEntries().get(index.get(k)));
                                }
                            }
                            rows.add(new Row(newRow));
                        }
                    }
                }
            }else if(tableComparator.GT()!=null) {
                for(int i=0;i<table1Row.size();i++) {
                    for (int j = 0; j < table2Row.size(); j++) {
                        if (table1Row.get(i).getEntries().get(table1ConditionIndex).compareTo(table2Row.get(j).getEntries().get(table2ConditionIndex))>0) {
                            ArrayList<Cell>newRow=new ArrayList<>();
                            for(int k=0;k<whichTable.size();k++){
                                if(whichTable.get(k)==1){
                                    newRow.add(table1Row.get(i).getEntries().get(index.get(k)));
                                }else {
                                    newRow.add(table2Row.get(j).getEntries().get(index.get(k)));
                                }
                            }
                            rows.add(new Row(newRow));
                        }
                    }
                }
            }
            return new QueryResult(new QueryTable[]{new QueryTable(columns,rows)});
        }
    }

    /**
     退出
     */
    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        try {
            manager.quit();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Quit.";
    }

    /**
     展示表格模式信息
     */
    @Override
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText().toLowerCase();
            String str = "Show table " + tableName +"\n-----------------------------------\n";
            Table table = GetCurrentDB().get(tableName);
            for (int i = 0; i < table.columns.size(); i++){
                Column column = table.columns.get(i);
                str += " " + column.getColumnName()
                     + " \t\t " + column.getColumnType() +(column.getColumnType()==STRING ? "(" + column.getMaxLength() + ")":"" )
                     + " \t\t " + (column.isPrimary()?"Primary Key":"")
                     + " \t\t " + (column.cantBeNull()?"Not Null":"")+    "\n";
            }
            str += "-----------------------------------\n";
            str += tableName + " has "+ table.columns.size() + " column(s). The primary key is " + table.columns.get(table.getPrimaryIndex()).getColumnName() + ".\n";
            return str;
        } catch (Exception e) {
            return e.getMessage();
        }

    }
}
