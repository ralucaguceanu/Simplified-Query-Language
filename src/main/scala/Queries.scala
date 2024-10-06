object Queries {

  def killJackSparrow(t: Table): Option[Table] = queryT(Some(t), "FILTER", Field("name", _ != "Jack"))

  def insertLinesThenSort(db: Database): Option[Table] =
      queryT(
        queryT(
          Some(queryDB(Some(db), "CREATE", "Inserted Fellas").get.tables.filter(t => t.name == "Inserted Fellas").head), 
          "INSERT", 
          List(
            Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
            Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
            Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
            Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")
          )
        ),
        "SORT",
        "age"
      )
    

  def youngAdultHobbiesJ(db: Database): Option[Table] = 
    queryT(
      queryT(
        Some(queryDB(Some(db), "JOIN", "People", "name", "Hobbies", "name").get.tables.head), 
        "FILTER", 
        Compound(_ && _, List(Field("age", _ < "25"), Field("name", _.startsWith("J")), Field("hobby", _ != "")))
      ),
      "EXTRACT", 
      List("name", "hobby")
    )
}
