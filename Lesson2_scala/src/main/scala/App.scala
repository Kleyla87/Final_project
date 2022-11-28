import scala.io.StdIn.readInt

object App {
  def main(args: Array[String]): Unit = {
    val phrase = "Hello, Scala!"
    val adding = " and goodbye python!"
    println("Задание а")
    println(phrase)
    println(phrase.reverse)
    println(phrase.toLowerCase)
    println(phrase.replace("!", ""))
    println(phrase.init + adding)
    println()
    println("Задание b")
    println("Введите годовой доход сотрудника:")
    val YearSalary = readInt()
    println("Введите размер премии (в процентах от годового дохода):")
    val bonus = readInt()
    println("Введите компенсацию питания сотрудника:")
    val eating = readInt()
    var MonthSalary = ((YearSalary + YearSalary * bonus / 100 + eating) * 0.87) / 12
    println(s"Ежемесячный оклад сотрудника: ${MonthSalary.toInt}")
    println()
    println("Задание c")
    val ListSalary = List(100, 150, 200, 80, 120, 75)
    val sumSalary = ListSalary.sum
    val numbers = ListSalary.length
    val meanSalary = sumSalary / numbers
    var variance: List[Int] = List()
    for (sal <- ListSalary) {
      val pr = (sal - meanSalary) * 100 / sal
      variance = variance ++ List(pr)
    }
    println(s"Отклонения размеров окладов сотрудников от среднего: $variance")
    val VarSalary = (MonthSalary - meanSalary) * 100 / MonthSalary
    println(s"Отклонения оклада нового сотрудника от среднего: ${VarSalary.toInt}")
    println()
    println("Задание d")
    MonthSalary = (MonthSalary - MonthSalary * VarSalary / 100).toInt
    val NewListSalary = ListSalary ++ List(MonthSalary.toInt)
    println(NewListSalary)
    val MaxSalary = NewListSalary.max
    val MinSalary = NewListSalary.min
    println(s"Самая высокая зарплата: $MaxSalary")
    println(s"Самая низкая зарплата: $MinSalary")
    println()
    println("Задание e")
    var SalaryList = NewListSalary ++ List(350, 90)
    SalaryList = SalaryList.sorted
    println(s"Отсортированный список зарплат: $SalaryList")
    println()
    println("Задание f")
    val NewWorker = 130
    val NewIndex = SalaryList.indexWhere(element => element > NewWorker)
    SalaryList = SalaryList.patch(NewIndex, List(NewWorker), 0)
    println(s"Список зарплат с новым работником: $SalaryList")
    println()
    println("Задание g")
    println("Введите минимальную зарплату уровня middle:")
    val MinMiddle = readInt()
    println("Введите максимальную зарплату уровня middle:")
    val MaxMiddle = readInt()
    val StartIndex = SalaryList.indexWhere(element => element >= MinMiddle)
    val EndIndex = SalaryList.lastIndexWhere(element => element <= MaxMiddle)
    val IndexMiddle = Range.inclusive(StartIndex, EndIndex).toList
    println(s"Номера сотрудников, попадающих в категорию middle: $IndexMiddle")
    println()
    println("Задание h")
    var IndexedSalary : List[Int] = List()
    for (sal <- SalaryList){
      val ind = (sal * 1.07).toInt
      IndexedSalary = IndexedSalary ++ List(ind)
    }
    println(s"Проиндексированные зарплаты: $IndexedSalary")
  }
}

