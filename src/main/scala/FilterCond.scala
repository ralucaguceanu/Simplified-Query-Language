import scala.language.implicitConversions

trait FilterCond {def eval(r: Row): Option[Boolean]}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = Some(predicate(r(colName)))
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    conditions match {
      case Nil => None
      case x :: xs =>
        val initialValue = x.eval(r)
        xs.foldLeft(initialValue)((acc, condition) =>
          (acc, condition.eval(r)) match {
            case (Some(result1), Some(result2)) => Some(op(result1, result2))
            case _ => None
          }
        )
    }
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = f.eval(r) match {
    case Some(result) => Some(!result)
    case _ => None
  }
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ && _, List(f1, f2))
def Or(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ || _, List(f1, f2))
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ == _, List(f1, f2))

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = fs.map(_.eval(r))
    if (results.contains(Some(true))) {
      Some(true)
    } else {
      Some(false)
    }
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = fs.map(_.eval(r))
    if (results.contains(Some(false))) {
      Some(false)
    } else {
      Some(true)
    }
  }
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = {
  val (colName, predicate) = t
  Field(colName, predicate)
}

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}