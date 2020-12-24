package demo.model

data class Order(
        val id: String,
        val products: List<String>?,
        val price: Int?) {
    // TODO sort out why BEAM Coder needs empty constructor
    constructor() : this("", emptyList(), 0)
}