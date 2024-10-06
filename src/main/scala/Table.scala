type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {

  override def toString: String = {

    // daca suntem la prima coloana punem direct numele acesteia, altfel delimitam prin virgula
    val columnNames = header.foldLeft("")((acc, column) => acc + (if (acc.isEmpty) "" else ",") + column)

    // delimitam intrarile prin \n si valorile fiecarei coloane prin virgula
    val entries = tableData.foldLeft("")((acc, line) =>
      acc + (if (acc.isEmpty) "" else "\n") + line.values.foldLeft("")((lineAcc, value) =>
        lineAcc + (if (lineAcc.isEmpty) "" else ",") + value
      )
    )

    columnNames + "\n" + entries
  }

  def insert(row: Row): Table = {
    if (!tableData.contains(row)) {
      new Table(tableName, tableData ::: List(row))
    } else {
      new Table(tableName, tableData)
    }
  }

  def delete(row: Row): Table = {
    if (!tableData.contains(row)) {
      new Table(tableName, tableData)
    } else {
      new Table(tableName, tableData.filter(entry => entry != row))
    }
  }

  // sortam dupa valorile de la cheia ceruta
  def sort(column: String): Table = new Table(tableName, tableData.sortBy(row => row(column)))

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val updatedData = tableData.map(entry =>
      if (f.eval(entry) == Some(true)) {
        val updatedRow = entry ++ updates
        updatedRow
      } else {
        entry
      }
    )
    new Table(tableName, updatedData)
  }

  def filter(f: FilterCond): Table = {
    val filteredData = tableData.filter(entry => f.eval(entry) == Some(true))
    new Table(tableName, filteredData)
  }

  def select(columns: List[String]): Table = {
    val selectedColumns = columns.foldLeft(List.fill(tableData.length)(Map[String, String]()))((acc, currCol) =>
      val columnIndex = header.indexOf(currCol)
      val selectedColumn = tableData.map(entry => entry.drop(columnIndex).take(1))
      acc.zip(selectedColumn).map(p => p._1 ++ p._2)
    )
    Table(tableName, selectedColumns)
  }
  
  def header: List[String] = tableData.head.keys.toList
  def data: Tabular = tableData
  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    val lines = s.split("\n").map(_.split(",").toList)  // liniile tabelului ca lista de liste de stringuri
    val header = lines.head  // headerul

    // adaugam fiecare valoare din fiecare linie cu antetul corespunzator intr un hashmap si apoi formam o lista cu acestea
    val entries = lines.tail.map(values => header.zip(values).toMap).toList

    new Table(name, entries)
  }
}

extension (table: Table) {
  def apply(i: Int): Table = Table(table.name, List(table.tableData(i)))
}
