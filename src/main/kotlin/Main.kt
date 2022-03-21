import com.mongodb.rx.client.MongoClients
import com.mongodb.rx.client.Success
import io.netty.handler.codec.http.HttpResponseStatus
import io.reactivex.netty.protocol.http.server.HttpServer
import org.bson.Document
import rx.Observable


enum class Currency {
    RUB,
    EURO,
    USD,
    UNDEFINED
}

// enum.valueOf() throws exception on fail
fun strToCurrency(str: String): Currency =
    when (str) {
        "RUB" -> Currency.RUB
        "EURO" -> Currency.EURO
        "USD" -> Currency.USD
        else -> Currency.UNDEFINED
    }

val toRub = mapOf(
    Currency.RUB to 1.0,
    Currency.USD to 30.0,
    Currency.EURO to 40.0
)

data class User(
    val id: Int,
    val currency: Currency
)

data class Product(
    val name: String,
    val kind: String,
    val priceInRub: Double
)


class MongoClient {
    val client = MongoClients.create("mongodb://localhost:27017")
    val db = client.getDatabase("db")

    fun usersCollection() = db.getCollection("users")
    fun productsCollection() = db.getCollection("products")

    fun addUser(id: Int, cStr: String) = usersCollection().insertOne(
        Document("id", id.toDouble()).append("currency", cStr)
    )

    fun addProduct(p: Product) = productsCollection().insertOne(
        Document("name", p.name)
            .append("kind", p.kind)
            .append("price", p.priceInRub)
    )

    fun request(kind: String, userId: Int): Observable<Pair<Product, Double>> {
        val result = productsCollection().find().toObservable().map { d ->
            Product(d.getString("name"), d.getString("kind"), d.getDouble("price"))
        }.filter { p -> p.kind == kind }.replay()
        result.connect()
        val parsingSub = result.subscribe() // warm "cold" stream

        return usersCollection().find().toObservable().map { d ->
            User(d.getDouble("id").toInt(), strToCurrency(d.getString("currency")))
        }.firstOrDefault(null) {
            it.id == userId
        }.flatMap { user ->
            if (user == null) {
                parsingSub.unsubscribe() // stop warming
                Observable.empty()
            } else {
                val ratio = if (user.currency == Currency.UNDEFINED)
                    1.0 else toRub[user.currency]!!
                result.map { p -> p to ratio }
            }
        }
    }
}


fun main() {
    val client = MongoClient()
    HttpServer.newServer(8080).start { req, resp ->
        resp.writeString(when (req.decodedPath) {
            "/get" -> {
                val id = req.queryParameters["userId"]!!.single().toInt()
                val kind = req.queryParameters["kind"]!!.single()
                client.request(kind, id).map { (product, ratio) ->
                    "${product.name.padEnd(30)}  ${product.priceInRub / ratio}\n"
                }
            }
            "/addUser" -> {
                val id = req.queryParameters["id"]!!.single().toInt()
                val cStr = req.queryParameters["currency"]!!.single()
                client.addUser(id, cStr).map(Success::toString)
            }
            "/addProduct" -> {
                val name = req.queryParameters["name"]!!.single()
                val kind = req.queryParameters["kind"]!!.single()
                val price = req.queryParameters["price"]!!.single().toDouble()
                client.addProduct(Product(name, kind, price)).map(Success::toString)
            }
            else -> {
                resp.status = HttpResponseStatus.NOT_FOUND
                Observable.just("Invalid query path")
            }
        })
    }.awaitShutdown()
}