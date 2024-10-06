case class Database(tables: List[Table]) {

  override def toString: String =  {
    tables.foldLeft("")((acc, table) =>
      val tableName = table.name + "\n"
      val tableContent = table.toString
      acc + (if (acc.isEmpty) "" else "\n\n") + tableName + tableContent
    )
  }

  def create(tableName: String): Database =
    if (tables.map(_.name).contains(tableName)) {
      this
    } else if (tables.isEmpty) {
      val newTable = Table(tableName, "")
        Database(List(newTable))
    } else {
      val newTable = Table(tableName, "")
      Database(tables ::: List(newTable))
    }

  def drop(tableName: String): Database = Database(tables.filter(t => t.name != tableName))

  def selectTables(tableNames: List[String]): Option[Database] = {
    val containsAll = tableNames.forall(name => tables.map(_.name).contains(name))
    if (!containsAll) {
      None
    } else {
      val selectedTables = tables.filter(table => tableNames.contains(table.name))
      Some(Database(selectedTables))
    }
  }

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    if (!tables.map(_.name).contains(table1) || !tables.map(_.name).contains(table2)) {
      return None
    }

    val tableA = tables.filter(table => table.name == table1).head
    val tableB = tables.filter(table => table.name == table2).head

    if (tableA.data.isEmpty) {
      return Some(tableB)
    } else if (tableB.data.isEmpty) {
      return Some(tableA)
    }

    val commonEntries : List[Row]  = tableA.data.map(rowA => {
      val commonValue = rowA.getOrElse(c1, "")
      val matchingRowsB = tableB.data.filter(rowB => rowB.getOrElse(c2, "") == commonValue)
      if (matchingRowsB.nonEmpty) {
        val matchingRowB = matchingRowsB.head
        
        // excludem coloana de join (c2) din row-ul gasit in tabelul B
        val rowBWithoutC2 = matchingRowB.filter((key, value) => key != c2)
        
        // pentru cazurile in care avem mai multe chei identice in tabelele A si B, concatenam valorile
        // din B ale cheilor la valorile din A ale cheilor
        val updatedRowA = rowA.foldLeft(Map.empty[String, String]) { (acc, field) =>
          val (keyA, valueA) = field
          val valueB = rowBWithoutC2.getOrElse(keyA, "")
          if (valueB == "" || valueB == valueA) {
            acc + field
          } else {
            acc + (keyA -> (valueA + ";" + valueB))
          }
        }

        // pentru cazurile in care avem chei identice in tabelele A si B, scoatem cheile respective din B,
        // intrucat valoarea actualizata deja se afla in tabelul A
        val updatedRowB = rowBWithoutC2.filter((key, _) => !updatedRowA.contains(key))

        val mergedRow = updatedRowA ++ updatedRowB
        Some(mergedRow)
      } else {
        None
      }
    }).filter(_.isDefined).map(_.get)
    
    val header = commonEntries.head.keys.toList
    val templateRow = header.map(key => key -> "").toMap

    val onlyInA = tableA.data.foldLeft(List.empty[Row]) { (acc, rowA) =>
      if (!tableB.data.exists(rowB => rowB.getOrElse(c2, "") == rowA.getOrElse(c1, ""))) {
        val populatedRow = templateRow ++ rowA
        acc :+ populatedRow
      } else {
        acc
      }
    }

    val onlyInB = tableB.data.foldLeft(List.empty[Row]) { (acc, rowB) =>
      if (!tableA.data.exists(rowA => rowA.getOrElse(c1, "") == rowB.getOrElse(c2, ""))) {
        // schimbam numele coloanei c2 in c1 pentru a se potrivi cu coloana din primul tabel
        val adjustedRowB = rowB - c2 + (c1 -> rowB.getOrElse(c2, ""))
        val populatedRow = templateRow ++ adjustedRowB
        acc :+ populatedRow
      } else {
        acc
      }
    }

    val joinedTable: Tabular = commonEntries ::: onlyInA ::: onlyInB
    Some(new Table("joinAB", joinedTable))
  }

  def apply(i: Int): Table = tables(i)
}
