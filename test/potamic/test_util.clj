(ns potamic.test-util
  (:require [potamic.connection :refer [pcar]]
            [potamic.db :as db]
            [potamic.queue :as q]
            [potamic.queue.queues :refer [queues_]]
            [taoensso.carmine :as car]))

(def valid-redis-uris
  ["redis://localhost:6379/0"
   "redis://127.0.0.1:6379/0"
   "redis://PASS@localhost:6379"
   "redis://PASS@localhost:6379/0"
   "redis://:PASS@127.0.0.1:6379"
   "redis://:PASS@127.0.0.1:6379/0"
   "redis://USER:PASS@127.0.0.1:6379"
   "redis://USER:PASS@127.0.0.1:6379/0"])

(def testable-redis-uris
  ["redis://default:secret@localhost:6379/0"
   "redis://default:secret@127.0.0.1:6379/0"])

(def valid-kvrocks-standalone-uris
  ["redis://localhost:6666/0"
   "redis://127.0.0.1:6666/0"
   "redis://PASS@localhost:6666"
   "redis://PASS@localhost:6666/0"
   "redis://:PASS@127.0.0.1:6666"
   "redis://:PASS@127.0.0.1:6666/0"
   "redis://USER:PASS@127.0.0.1:6666"
   "redis://USER:PASS@127.0.0.1:6666/0"])

(def testable-kvrocks-standalone-uris
  ["redis://default:secret@localhost:6666/0"
   "redis://default:secret@127.0.0.1:6666/0"])

(def valid-kvrocks-cluster-uris
  ["redis://localhost:6669/0"
   "redis://127.0.0.1:6669/0"
   "redis://PASS@localhost:6669"
   "redis://PASS@localhost:6669/0"
   "redis://:PASS@127.0.0.1:6669"
   "redis://:PASS@127.0.0.1:6669/0"
   "redis://USER:PASS@127.0.0.1:6669"
   "redis://USER:PASS@127.0.0.1:6669/0"])

(def testable-kvrocks-cluster-uris
  ["redis://default:secret@localhost:6669/0"
   "redis://default:secret@127.0.0.1:6669/0"])

(def valid-uri-parse-mappings
  "Map of the form: `{REDIS-URI CLJ-HASH}`."
  {"redis://localhost:6379/0"
   {:scheme "redis" :host "localhost" :user nil :password nil :port 6379 :db 0}

  "redis://USER:PASS@localhost:1234"
  {:scheme "redis" :host "localhost" :user "USER" :password "PASS" :port 1234 :db nil}

  "redis://scooby:doo@123.124.125.126:6666/1"
  {:scheme "redis" :host "123.124.125.126" :user "scooby" :password "doo" :port 6666 :db 1}})

(def uri-redis-standalone   "redis://default:secret@localhost:6379/0")
(def uri-kvrocks-standalone "redis://default:secret@localhost:6666/0")
(def uri-kvrocks-cluster    "redis://default:secret@localhost:6669/0")

(declare conn-redis-standalone
         conn-kvrocks-standalone
         conn-kvrocks-cluster)

(defn fx-make-conns
  [f]
  (alter-var-root #'conn-redis-standalone
                  (constantly (db/make-conn :uri uri-redis-standalone)))
  (alter-var-root #'conn-kvrocks-standalone
                  (constantly (db/make-conn :backend :kvrocks :uri uri-kvrocks-standalone)))
  (alter-var-root #'conn-kvrocks-cluster
                  (constantly (db/make-conn :backend :kvrocks :uri uri-kvrocks-cluster)))
  (f))

(def test-queue :my/test-queue)
(def test-queue-group :my/test-queue-group)

(def id-pat #"\d+-\d+")

(defn fx-prime-db
  [f]
  (letfn [(-destroy-redis-standalone []
            (q/destroy-queue! test-queue conn-redis-standalone :unsafe true)
            (pcar conn-redis-standalone (car/flushall)))
          (-destroy-kvrocks-standalone []
            (q/destroy-queue! test-queue conn-kvrocks-standalone :unsafe true)
            (pcar conn-kvrocks-standalone (car/flushall)))
          (-destroy-kvrocks-cluster []
            (q/destroy-queue! test-queue conn-kvrocks-cluster :unsafe true)
            (pcar conn-kvrocks-cluster (car/flushall)))
          (-create-test-queue [conn]
            (q/create-queue! test-queue conn))
          (-reset-queues []
            (reset! queues_ nil))]
    (-destroy-redis-standalone)
    (-destroy-kvrocks-standalone)
    (-destroy-kvrocks-cluster)
    (-reset-queues)
    (-create-test-queue conn-redis-standalone)
    (f)))

(defmacro pcar-redis-standalone
  [& body]
  `(pcar potamic.test-util/conn-redis-standalone ~@body))

(defmacro pcar-kvrocks-standalone
  [& body]
  `(pcar potamic.test-util/conn-kvrocks-standalone ~@body))

(defmacro pcar-kvrocks-cluster
  [& body]
  `(pcar potamic.test-util/conn-kvrocks-cluster ~@body))
