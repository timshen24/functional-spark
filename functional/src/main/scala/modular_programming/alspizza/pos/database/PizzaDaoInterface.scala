package modular_programming.alspizza.pos.database

import modular_programming.alspizza.Money
import modular_programming.alspizza.pos.model._

trait PizzaDaoInterface {

  def getToppingPrices: Map[Topping, Money]
  def getCrustSizePrices: Map[CrustSize, Money]
  def getCrustTypePrices: Map[CrustType, Money]

}