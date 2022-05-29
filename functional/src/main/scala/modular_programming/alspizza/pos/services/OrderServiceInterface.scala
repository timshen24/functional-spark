package modular_programming.alspizza.pos.services

import modular_programming.alspizza.Money
import modular_programming.alspizza.pos.database.PizzaDaoInterface
import modular_programming.alspizza.pos.model.Order

trait OrderServiceInterface {
  // implementing classes should provide their own database
  // that is an instance of PizzaDaoInterface, such as
  // MockPizzaDao, TestPizzaDao, or ProductionPizzaDao
  protected def database: PizzaDaoInterface

  def calculateOrderPrice(o: Order): Money
}
