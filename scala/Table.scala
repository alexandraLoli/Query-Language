import util.Util.Row
import util.Util.Line

import scala.math.Ordering.Implicits.seqOrdering
//import TestTables.tableImperative
//import TestTables.tableFunctional
//import TestTables.tableObjectOriented

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    if (!r.contains(colName))
      None
    else {
      Some(predicate(r(colName)))
    }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    for {
      ok1 <- f1.eval(r)
      ok2 <- f2.eval(r)
    } yield ok1 && ok2
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    for {
      ok1 <- f1.eval(r)
      ok2 <- f2.eval(r)
    } yield ok1 || ok2
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] =
    if (target.eval.isEmpty)
        None
    else
      target.eval.get.select(columns)
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] =
    if (target.eval.isEmpty)
      None
    else
      target.eval.get.filter(condition)
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] =
    if (target.eval.isEmpty)
      None
    else
      Some(target.eval.get.newCol(name, defaultVal))
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] =
    if (t1.eval.isEmpty || t2.eval.isEmpty)
      None
    else
      t1.eval.get.merge(key,t2.eval.get)
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular

  // 1.1
  override def toString: String =
    (columnNames.mkString(",") :: tabular.map(_.mkString(","))).mkString("\n")


  // 2.1
  def select(columns: Line): Option[Table] = {
    val existingColumns = columns.filter(columnNames.contains(_))
    if (existingColumns == Nil)
      None
    else {
      def changeToIndex(x: String) = columnNames.indexOf(x)
      val indexes = columns.map(changeToIndex)
      val newTable = tabular.map(_.zipWithIndex.filter(pair => indexes.contains(pair._2)).map(_._1))
      Some(new Table(existingColumns,newTable))
    }
  }

  //  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    val tableWithColumnNames = tabular.map(row => columnNames.zip(row).toMap)

    def extractBoolean(bool: Option[Boolean]): Boolean =
      if (bool.isEmpty)
        false
      else if (bool.contains(false))
        false
      else true

    val goodTable = tabular.zip(tableWithColumnNames).filter(pair => extractBoolean(cond.eval(pair._2))).map(_._1)
    if (goodTable == Nil)
      None
    else Some(new Table(columnNames, goodTable))
  }


  //  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    val listWithName: List[String] = List(name)
    val listWithValue: List[String] = List(defaultVal)
    val updatedColumns: List[String] = columnNames ++ listWithName

    def appendToList(list: List[String]): List[String] =
      list ++ listWithValue

    val updatedTabular  = tabular.map(appendToList)
    new Table(updatedColumns, updatedTabular)
  }

  //  // 2.4.
  def merge(key: String, other: Table): Option[Table] =
    if(!this.columnNames.contains(key) || !other.getColumnNames.contains(key))
      None
    else {
      val columnsMerged = (this.getColumnNames ++ other.getColumnNames).distinct
      val table1Map = this.getTabular.map(row => this.getColumnNames.zip(row).toMap)
      val table2Map = other.getTabular.map(row => other.getColumnNames.zip(row).toMap)


      def findIfElemIsInBoth(elem: String, index: Int, table: List[Map[String, String]]): Row = {
        if (index == table.size)
          Map.empty
        else {
          val row1 = table(index)
          if (row1(key) == elem)
            row1
          else
            findIfElemIsInBoth(elem, index + 1, table)
        }
      }

      val mergedTables1 : List[Map[String, String]] = for {
        row1 <- table1Map
      } yield {
        val row2 =  findIfElemIsInBoth(row1(key), 0, table2Map)
        if (row2 != Map.empty) {
          columnsMerged.map {
            col =>
              val v1 = row1.getOrElse(col, "")
              val v2 = row2.getOrElse(col, "")
              if(col.equals(key) || v1.equals(v2))
                col -> v1
              else {
                if(v1.equals("") && !v2.equals(""))
                  col-> v2
                else if(v2.equals("") && !v1.equals(""))
                  col -> v1
                else
                  col -> s"$v1;$v2"
              }
          }.toMap : Map[String, String]
        }
        else
          columnsMerged.map {
            col =>
              val v1 = row1.getOrElse(col, "")
              col -> v1
          }.toMap : Map[String, String]
      }

      val mergedTables2: List[Map[String, String]] = for {
        row2 <- table2Map
      } yield {
        val row1 = findIfElemIsInBoth(row2(key), 0, table1Map)
        if (row1 != Map.empty) {
          Map.empty
        }
        else {
          columnsMerged.map {
            col =>
              val v1 = row2.getOrElse(col, "")
              col -> v1
          }.toMap
        }
      }

      def orderList(hashMap: Map[String, String], index: Int, acc: List[String]): List[String] = {
        if(index == columnsMerged.size)  acc
        else {
          orderList(hashMap, index + 1, acc ++ List(hashMap(columnsMerged(index))))
        }
      }
      val mergedTablesFinal = (mergedTables1 ++ mergedTables2).filter(_.nonEmpty).map(orderList(_, 0, Nil:List[String]))
      Some(new Table(columnsMerged, mergedTablesFinal))
    }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val lists = s.split("\n").map(line => line.split(",", -1).toList).toList
    new Table(lists.head, lists.drop(1))
  }
}
