package demo.model

data class Order(
        val id: String,
        val products: List<String>?,
        val price: Int?
)