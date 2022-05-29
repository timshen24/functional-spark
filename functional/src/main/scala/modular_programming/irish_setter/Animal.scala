package modular_programming.irish_setter

import java.awt.Color

trait Animal

abstract class AnimalWithTail(tailColor: Color) extends Animal

trait DogTailService {
  this: AnimalWithTail =>
  def wagTail(): Unit = println("Wagging tail")
  def lowerTail(): Unit = println("Lower tail")
  def raiseTail(): Unit = println("Raise tail")
  def curlTail(): Unit = println("Curl tail")
}

trait DogMouthService {
  this: AnimalWithTail =>
  def bark(): Unit = println("Bark!")
  def lick(): Unit = println("Lick!")
}

object IrishSetter extends AnimalWithTail(tailColor = Color.RED) with DogTailService with DogMouthService {
  def main(args: Array[String]): Unit = {
    bark()
    wagTail()
  }
}