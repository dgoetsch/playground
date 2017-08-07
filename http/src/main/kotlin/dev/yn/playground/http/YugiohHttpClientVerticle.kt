package dev.yn.playground.http

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.core.CompositeFuture
import io.vertx.core.file.FileSystem

data class HttpClientConfig(
        val host: String,
        val port: Int? = 443,
        val ssl: Boolean = true,
        val URI: String = "/",
        val headers: MutableMap<String, Iterable<String>> = mutableMapOf()
)
class YugiohHttpClientVerticle(val config: HttpClientConfig): AbstractVerticle() {

    val LOG = LoggerFactory.getLogger(this.javaClass)
    val requestOptions: RequestOptions = RequestOptions()
            .setHost(config.host)
            .setURI(config.URI)
            .setSsl(config.ssl)
            .let { config.port?.let { port -> it.setPort(port) }?:it }

    override fun start() {
        val client = vertx.createHttpClient(HttpClientOptions().setSsl(config.ssl))
        val yugiohClient = YuGiOhClientRequests(client, config)

        yugiohClient.getSets()
                .compose { setNames ->
                    FutureSequence(setNames.take(10).map { setName ->
                        when (setName) {
                            is String -> yugiohClient.getSetData(setName)
                            else -> ClientError.failedFuture<JsonObject>(ClientError.ParseError("could not handle set name", setName))
                        } }) }
                .map{it -> println(it)}
                .recover { it ->
                    println(it)
                    Future.failedFuture("failed")
                }
//        val fileSystem = vertx.fileSystem()
//        fileSystem.mkdirBlocking("sets/")
//        fileSystem.mkdirBlocking("cards/")
//
//        val fileService = FileService(fileSystem)
//        val service = YuGiOhPricesService(fileService, yugiohClient)
//
//
//        yugiohClient.getSets()
//                .compose { setNames -> service.processSetNames(setNames, service::cacheSetData) }
//
//        yugiohClient.getSets()
//                .compose { setNames ->  service.processSetNamesB(setNames, yugiohClient::getSetData) }
//                .compose { sets -> FutureSequence.lift(sets.map {
//                    service.processSetDataB(it, {
//                        it.getString("name")?.let(yugiohClient::getCardData)
//                                ?:ClientError.failedFuture<JsonObject>(ClientError.ParseError("Expected 'name' field", it))
//                })}) }
//                .map{ cards ->
//                    cards.forEach(::println)
//                    cards
//                }
    }
}

class YuGiOhPricesService(val fileService: FileService, val yuGiOhClientRequests: YuGiOhClientRequests) {
    fun <T> processSetNames(setNames: JsonArray, doWithEachSetName: (String) -> Future<T>): CompositeFuture =
            CompositeFuture.all(setNames.map { setName ->
                when (setName) {
                    is String -> doWithEachSetName(setName)
                    else -> ClientError.failedFuture<String>(ClientError.ParseError("could not handle set name", setName))
                }
            })

    fun cacheSetData(setName: String): Future<CompositeFuture> {
        return yuGiOhClientRequests.getSetData(setName)
                .compose(fileService.makeDirectors(listOf("sets/$setName", "cards/$setName")))
                .compose { setData -> processSetData(setData, { setCardData ->
                    fileService.writeFile<JsonObject>("sets/$setName/${setCardData.getString("name")}.json", setCardData.toBuffer(), setCardData)
                            .compose { setCardData -> setCardData.getString("name")?.let(yuGiOhClientRequests::getCardData)?:ClientError.failedFuture(ClientError.ParseError("Expected 'name' field", setCardData)) }
                            .compose { cardData -> cardData.getString("name")?.let { fileService.writeFile("cards/$setName/$it.json", cardData.toBuffer(), cardData) } }
                }) }
    }

    fun <T> processSetData(setData: JsonObject, doWithCard: (JsonObject) -> Future<T>): Future<CompositeFuture> =
            when (setData.getString("status")) {
                "success" ->
                    CompositeFuture.all(setData
                            .getJsonObject("data")
                            ?.getJsonArray("cards")
                            ?.map {
                                when (it) {
                                    is JsonObject -> doWithCard(it)
                                    else -> ClientError.failedFuture<Unit>(ClientError.ParseError("card is not an object", it))
                                }
                            }) ?:ClientError.failedFuture<CompositeFuture>(ClientError.ParseError("could not parse set data", setData))
                else -> ClientError.failedFuture<CompositeFuture>(ClientError.ErrorResponse("response status was not a success", setData))
            }

    fun <T> processSetDataB(setData: JsonObject, doWithCard: (JsonObject) -> Future<T>): Future<List<T>> =
            when (setData.getString("status")) {
                "success" ->
                    setData
                            .getJsonObject("data")
                            ?.getJsonArray("cards")
                            ?.let { array ->
                                FutureSequence(array.map {
                                    when (it) {
                                        is JsonObject -> doWithCard(it)
                                        else -> ClientError.failedFuture<T>(ClientError.ParseError("card is not an object", it))
                                    }
                                })
                            } ?:ClientError.failedFuture<List<T>>(ClientError.ParseError("could not parse set data", setData))
                else -> ClientError.failedFuture<List<T>>(ClientError.ErrorResponse("response status was not a success", setData))
            }

    fun <T> processSetNamesB(setNames: JsonArray, doWithSetName: (String) -> Future<T>): Future<List<T>> =
            FutureSequence<T>(setNames.map { setName ->
                when (setName) {
                    is String -> doWithSetName(setName)
                    else -> ClientError.failedFuture<T>(ClientError.ParseError("could not handle set name", setName))
                }
            })
}
class FileService(val fileSystem: FileSystem) {
    val LOG = LoggerFactory.getLogger(this.javaClass)

    fun <T> makeDirectors(dirs: List<String>): (T) -> Future<T> = { input ->
        FutureSequence(dirs.map { dir ->
            val future = Future.future<Void>()
            fileSystem.mkdir(dir, future.completer())
            future.map {
                LOG.info("created directory: $dir")
                it
            }
        }).map { input }
    }

    fun <T> makeDirectors(dirs: List<String>, result: T): Future<T> {
        return FutureSequence(dirs.map { dir ->
            val future = Future.future<Void>()
            fileSystem.mkdir(dir, future.completer())
            future.map {
                LOG.info("created directory: $dir")
                it
            }
        }).map { result }
    }

    fun <T> writeFile(fileName: String, buffer: Buffer, result: T): Future<T> {
        val future = Future.future<Void>()
        fileSystem.writeFile(fileName, buffer, future.completer())
        return future.map {
            LOG.info("wrote file: $fileName")
            result
        }
    }
}

sealed class ClientError {
    data class Wrapper(val error: ClientError): Throwable()

    companion object {
        val logError: (ClientError) -> Unit = { clientError ->
            val LOG = LoggerFactory.getLogger(this.javaClass)

            when(clientError) {
                is ParseError -> {
                    LOG.error("Could not parse response because ${clientError.body}"
                     + "\n${clientError.body}")
                }
                is ErrorStatus -> LOG.error("Bad Response Status: ${clientError.httpClientResponse.statusCode()}")
                is ParseExceptionError -> {
                    LOG.error("Could not parse response: ${clientError.message}")
                    LOG.error(clientError.error)
                    LOG.debug(clientError.error.stackTrace.joinToString("\n\t\t"))
                }
                is ErrorResponse ->
                        LOG.error("Received an error response: ${clientError.message}" +
                                "\n${clientError.body}")

            }
        }

        fun <T> futureHandler(future: Future<T>): (ClientError) -> Unit = { future.fail(Wrapper(it)) }

        fun <T> failedFuture(clientError: ClientError): Future<T> = Future.failedFuture<T>(Wrapper(clientError))
    }
    class ParseError(val message: String, val body: Any): ClientError()
    class ErrorResponse(val message: String, val body: Any): ClientError()
    class ErrorStatus(val httpClientResponse: HttpClientResponse): ClientError()
    class ParseExceptionError(val message: String, val error: Throwable, body: Buffer, val httpClientResponse: HttpClientResponse): ClientError()
}
class YuGiOhClientRequests(val httpClient: HttpClient, val config: HttpClientConfig) {
    val LOG = LoggerFactory.getLogger(this.javaClass)

    val defaultHeaders: Map<String, Iterable<String>> = mapOf(
            "content-type" to listOf("application/json"))

    fun getSets(): Future<JsonArray> {
        val future: Future<JsonArray> = Future.future()
//        val handler = { jsonArray: JsonArray -> future.complete(jsonArray)
//            jsonArray.forEach {
//                when(it) {
//                    is String -> doWithSetName(it)
//                    else -> onError(ClientError.ParseError("could not handle set name", it))
//                }
//            }
//        }

        httpClient.get(
                RequestOptions()
                        .setHost(config.host)
                        .setURI("/api/card_sets")
                        .setSsl(config.ssl)
                        .let { config.port?.let { port -> it.setPort(port) }?:it })
                .handler(jsonArrayHandler(future::complete, ClientError.futureHandler(future)))
                .let(addHeaders(defaultHeaders))
                .let(addHeaders(config.headers))
                .end()

        return future
    }

    fun getSetData(setName: String): Future<JsonObject> {
        val future = Future.future<JsonObject>()

//        val handler: (JsonObject) -> Unit = {jsonObject: JsonObject ->
//            when(jsonObject.getString("status")) {
//                "success" ->
//                    jsonObject
//                            .getJsonObject("data")
//                            ?.getJsonArray("cards")
//                            ?.map{
//                                when(it) {
//                                    is JsonObject -> doWithCards(it)
//                                    else -> onError(ClientError.ParseError("card is not an object", it))
//                                }
//                            }?:onError(ClientError.ParseError("could not parse set data", jsonObject))
//                else -> onError(ClientError.ErrorResponse("response status was not a success", jsonObject))
//            }
//        }
        httpClient.get(
                RequestOptions()
                        .setHost(config.host)
                        .setURI("/api/set_data/${setName}")
                        .setSsl(config.ssl)
                        .let { config.port?.let { port -> it.setPort(port) }?:it })
                .handler(jsonObjectHandler(future::complete, ClientError.futureHandler(future)))
                .let(addHeaders(defaultHeaders))
                .let(addHeaders(config.headers))
                .end()

        return future
    }

    fun getCardData(cardName: String): Future<JsonObject> {
        val future = Future.future<JsonObject>()
        httpClient.get(
                RequestOptions()
                        .setHost(config.host)
                        .setURI("/api/card_data/${cardName}")
                        .setSsl(config.ssl)
                        .let { config.port?.let { port -> it.setPort(port) } ?: it })
                .handler(jsonObjectHandler(future::complete, ClientError.futureHandler(future)))
                .let(addHeaders(defaultHeaders))
                .let(addHeaders(config.headers))
                .end()

        return future
    }

    private fun <T> baseHandler(parse: (HttpClientResponse, Buffer) -> T):
            (HttpClientResponse, (T) -> Unit, (ClientError) -> Unit) -> Unit = {
        response: HttpClientResponse, handle: (T) -> Unit, onError: (ClientError) -> Unit ->
        when(response.statusCode()/100) {
            2 ->
                response.bodyHandler { body ->
                    try {
                        handle(parse(response, body))
                    } catch(e: Throwable) {
                        onError(ClientError.ParseExceptionError("could not parse body", e, body, response))
                    }
                }
            else -> onError(ClientError.ErrorStatus(response))
        }
    }

    private fun jsonArrayHandler(
            handle: (JsonArray) -> Unit,
            onError: (ClientError) -> Unit
    ): (HttpClientResponse) -> Unit  = { response: HttpClientResponse ->
        baseHandler { httpClientResponse, buffer ->  buffer.toJsonArray() } (response, handle, onError)
    }

    private fun jsonObjectHandler(
            handle: (JsonObject) -> Unit,
            onError: (ClientError) -> Unit
    ): (HttpClientResponse) -> Unit = { response: HttpClientResponse ->
        baseHandler { httpClientResponse, buffer ->  buffer.toJsonObject() } (response, handle, onError)
    }



    fun addHeaders(headers: Map<String, Iterable<String>>): (HttpClientRequest) -> HttpClientRequest  = { request ->
        headers.forEach { headerName, headerValues ->
            request.putHeader(headerName, headerValues)
        }
        request
    }
}