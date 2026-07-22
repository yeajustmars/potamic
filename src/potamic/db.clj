(ns potamic.db
  "Redis-compatible DB functionality."
  (:require [potamic.connection :as conn]
            [potamic.db.validation :as dbv]
            [potamic.errors :as e]
            [potamic.util :as pu]
            [potamic.validation :as v]
            [taoensso.carmine :as car :refer [wcar]])
  (:gen-class))

(defn- -make-redis-conn
  [uri pool]
  {:pool pool
   :spec {:backend :redis :uri uri}})

(def ^:private kvrocks-namespaces_
  "Just keeps track of created Kvrocks Namespaces for side-effect purposes.
  When `make-conn` is called, Kvrocks backend will be synced with a new namespace
  if it doesn't already exist. This tracks what's already been created."
  (atom #{}))

(defn- -make-kvrocks-conn
  "Makes a Kvrocks connection usable by Carmine. Kvrocks changes how Redis does databases
  (0-N) in that it creates password-protected namespaces instead of numbers. This function
  will mimic/duck-type Kvrocks into behaving like Redis by creating a Namespace of `N` with
  a password of `N` and then inject a `(car/auth N)` call into Carmine via the
  `potamic.connection/pcar` macro.

  See also:

  - `potamic.connection/pcar`"
  [uri pool]
  (let [uri-obj (pu/parse-redis-uri uri)
        spec (-> (select-keys uri-obj [:host :port :password :db])
                 (assoc :backend :kvrocks))
        conn {:pool pool
              :spec spec}
        db (:db spec)]
    (when-not (contains? @kvrocks-namespaces_ db)
      (wcar conn
            (car/auth (:password spec))
            (car/redis-call ["NAMESPACE" "ADD" db db]))
      (swap! kvrocks-namespaces_ conj db))
    conn))

(defn make-conn
  "Creates a connection for Redis. Returns `conn` or throws Potamic Error.
  On success, `conn` will be usable by `potamic.queue` and the underlying
  `taoensso.carmine/wcar` library.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db])

  (db/make-conn :uri \"redis://localhost:6379/0\")
  ;= {:spec
  ;=  {:uri \"redis://localhost:6379/0\"}
  ;=   :pool #taoensso.carmine.connections.ConnectionPool[..]}
  ```"
  [& opts]
  (let [{:keys [backend uri pool] :as args} (apply hash-map opts)]
    (if-let [args-err (v/invalidate dbv/ConnArgs args)]
      (let [err (e/error {:potamic/err-type :potamic/args-err
                          :potamic/err-fatal? true
                          :potamic/err-msg (str "Invalid args provided to "
                                                "potamic.db/make-conn")
                          :potamic/err-data {:args args :err args-err}})]
        (e/throw-potamic-error err))
      (let [pool (or pool (conn/make-connection-pool))]
        (case backend
          :kvrocks (-make-kvrocks-conn uri pool)
          (-make-redis-conn uri pool))))))

(defn key-exists?
  "Returns boolean after checking if key exists in DB.

  Examples:

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:spec
  ;=  {:uri \"redis://localhost:6379/0\"}
  ;=   :pool #taoensso.carmine.connections.ConnectionPool[..]}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (db/key-exists? :my/queue conn)
  ;= true
  ```"
  [k conn]
(> (wcar conn (car/exists (pu/->str k))) 0))
