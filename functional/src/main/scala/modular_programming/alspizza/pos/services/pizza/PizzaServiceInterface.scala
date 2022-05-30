package modular_programming.alspizza.pos.services.pizza

import modular_programming.alspizza.Money
import modular_programming.alspizza.pos.model.{CrustSize, CrustType, Pizza, Topping}

trait PizzaServiceInterface {
  def addTopping(p: Pizza, t: Topping): Pizza
  def removeTopping(p: Pizza, t: Topping): Pizza
  def removeAllToppings(p: Pizza): Pizza
  def updateCrustSize(p: Pizza, cs: CrustSize): Pizza
  def updateCrustType(p: Pizza, ct: CrustType): Pizza
  def calculatePizzaPrice(
                           p: Pizza,
                           toppingsPrices: Map[Topping, Money],
                           crustSizePrices: Map[CrustSize, Money],
                           crustTypePrices: Map[CrustType, Money]
                         ): Money
}
