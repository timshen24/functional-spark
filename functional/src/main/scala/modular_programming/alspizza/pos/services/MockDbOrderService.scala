package modular_programming.alspizza.pos.services

import modular_programming.alspizza.pos.database._

object MockDbOrderService extends AbstractOrderService {

  val database: PizzaDaoInterface = MockPizzaDao

}
