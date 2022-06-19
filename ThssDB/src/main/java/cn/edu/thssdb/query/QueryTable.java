package cn.edu.thssdb.query;

import cn.edu.thssdb.common.Pair;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterable<Row> {
  public List<String>columns;
  public List<Row>rows;

  QueryTable(List<String>columns, List<Row> rows) {
    // TODO
    this.columns=columns;
    this.rows=rows;
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }

  private class TableIterator implements Iterator<Row>{
    private  Iterator<Row> iterator;
    TableIterator(QueryTable queryTable){
      this.iterator=queryTable.rows.iterator();
    }
    @Override
    public boolean hasNext() {
      // TODO
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      // TODO
      return iterator.next();
    }
  }
}