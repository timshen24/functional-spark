package modular_programming.alspizza.pos.services.order

import modular_programming.alspizza.pos.database.{MockPizzaDao, PizzaDaoInterface}

object ServiceOfMockDbOrder extends OrderServiceAbstract {

  val database: PizzaDaoInterface = MockPizzaDao

}
