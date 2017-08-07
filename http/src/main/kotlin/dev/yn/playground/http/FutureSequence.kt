package dev.yn.playground.http

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.impl.NoStackTraceThrowable
import org.funktionale.option.Option
import org.funktionale.option.orElse

class FutureSequence<T>(val futures: List<Future<T>>): Future<List<T>>, Handler<AsyncResult<List<T>>> {
    var count = 0;
    var handler: Handler<AsyncResult<List<T>>>? = null
    var results: Array<Option<AsyncResult<T>>> = Array(futures.size, { Option.None })

    var completed: Boolean = false
    var cause: Throwable? = null

    init {
          futures.forEachIndexed { i: Int, f: Future<T> ->
              f.setHandler { ar ->
                  var handler: Handler<AsyncResult<List<T>>>? = null
                  println("completed future $i")
                  if(ar.succeeded())  {
                      synchronized(this) {
                          count ++
                          results.set(i, Option.Some(ar))
                          if(!isComplete && count == futures.size) {
                              handler = setComplete()
                          }
                      }
                  } else {
                      synchronized(this) {
                          if(!isComplete) {
                              handler = setFailed(ar.cause())
                          }
                      }
                  }

                  println("ar $i, succeeded: ${ar.succeeded()}, handler: $handler, completed: $completed")
                  handler?.let{ it.handle(resultsToResult()) }
              }
          }
        if(futures.isEmpty()) {
            setComplete()
        }
    }

    private fun resultsToResult(): AsyncResult<List<T>>? {
        return results.fold<Option<AsyncResult<T>>, Option<AsyncResult<List<T>>>>(Option.None as Option<AsyncResult<List<T>>>) {
            aggOpt: Option<AsyncResult<List<T>>>, arOpt: Option<AsyncResult<T>> ->
            when(aggOpt) {
                is Option.None -> arOpt.map { it.map { listOf(it)}}
                else -> aggOpt.filter { it.succeeded() }
                        .flatMap { aggRes ->
                            arOpt.filter { it.succeeded()}
                                    .map { ar -> aggRes.map { it + ar.result() } }
                                    .orElse { arOpt.map {it.map{it -> emptyList<T>()}} } }
                        .orElse { aggOpt }

            }
        }.orNull()
    }

    companion object {
        fun <T> noHandler(): Handler<AsyncResult<List<T>>> = Handler { }

        fun <T> lift(futures: List<Future<List<T>>>): Future<List<T>> {
            return futures.fold(Future.succeededFuture(emptyList<T>()), { future: Future<List<T>>, next: Future<List<T>> ->
                future.compose { agg ->
                    next.map { agg + it }
                }
            })
        }
    }

    private fun setComplete(): Handler<AsyncResult<List<T>>>? {
        synchronized(this) {
            if(isComplete()) {
                return null
            }
            this.completed = true
            return handler ?: noHandler()
        }
    }

    private fun setFailed(cause: Throwable):Handler<AsyncResult<List<T>>>? {
        synchronized(this) {
            if(isComplete()) {
                return null
            }
            this.completed = true
            this.cause = cause
            return handler ?: noHandler()
        }
    }

    override fun isComplete(): Boolean {
        return this.completed
    }

    override fun fail(failureMessage: String) {
        if (!tryFail(failureMessage)) {
            throw IllegalStateException("Result is already complete: " + if (this.cause == null) "succeeded" else "failed")
        }
    }

    override fun fail(cause: Throwable) {
        if (!tryFail(cause)) {
            throw IllegalStateException("Result is already complete: " + if (this.cause == null) "succeeded" else "failed")
        }
}

    override fun tryFail(cause: Throwable): Boolean {
        val handler = setFailed(cause)
        return handler?.let {
            handler.handle(resultsToResult())
            true
        }?:false
    }

    override fun tryFail(failureMessage: String): Boolean {
        return tryFail(NoStackTraceThrowable(failureMessage))
    }

    override fun setHandler(handler: Handler<AsyncResult<List<T>>>?): Future<List<T>> {
        var call: Boolean = false
        synchronized(this) {
            this.handler = handler
            call = completed
        }

        if(call) {
            handler?.handle(resultsToResult())
        }
        return this
    }

    override fun result(): List<T>? {
        return if(completed && cause == null) resultsToResult()?.result() else null
    }

    override fun tryComplete(result: List<T>): Boolean {
        val handler = setComplete()
        if(handler != null) {
            handler.handle(this)
            return true
        } else {
            return false
        }
    }

    override fun tryComplete(): Boolean {
        val handler = setComplete()
        if(handler != null) {
            handler.handle(resultsToResult())
            return true
        } else {
            return false
        }
    }

    override fun complete() {
        if (!tryComplete()) {
            throw IllegalStateException("Result is already complete: " + if (this.cause == null) "succeeded" else "failed")
        }
    }

    override fun complete(result: List<T>) {
        if (!tryComplete(result)) {
            throw IllegalStateException("Result is already complete: " + if (this.cause == null) "succeeded" else "failed")
        }
    }

    override fun cause(): Throwable? {
        return if(completed) cause else null
    }

    override fun failed(): Boolean {
        return completed && cause != null
    }

    override fun succeeded(): Boolean {
        return completed && cause == null
    }

    override fun handle(asyncResult: AsyncResult<List<T>>) {
       if(asyncResult.succeeded()) {
           complete(asyncResult.result())
       } else {
           fail(asyncResult.cause())
       }
    }

    override fun completer(): Handler<AsyncResult<List<T>>> = this

}