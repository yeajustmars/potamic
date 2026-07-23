(ns potamic.db
  "Key-value store functionality."
  (:require [potamic.connection :as conn :refer [pcar]]
            [potamic.db.validation :as dbv]
            [potamic.errors :as e]
            [potamic.util :as pu]
            [potamic.validation :as v]
            [taoensso.carmine :as car :refer [wcar]]
            [taoensso.timbre :as log]))

(defn- -make-redis-conn
  "Makes a standard Redis connection usable by Carmine."
  [uri pool skip-checks?]
  (let [conn {:pool pool :spec {:backend :redis :uri uri}}]
    (if skip-checks?
      conn
      (try
        (wcar conn (car/ping))
        conn
        (catch Exception e
          (let [err (e/error {:potamic/err-type :potamic/db-err
                              :potamic/err-fatal? true
                              :potamic/err-data {::err (Throwable->map e)}
                              :potamic/err-msg (str "Can't make Redis connection: "
                                                    (.getMessage e))})]
            (log/error err)
            (throw e)))))))

(def kvrocks-namespaces_
  "Just keeps track of created Kvrocks Namespaces for side-effect purposes.
  When `make-conn` is called for `:kvrocks` backend, a new namespace is created
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
  [uri pool skip-checks?]
  (let [uri-obj (pu/parse-redis-uri uri)
        spec (-> (select-keys uri-obj [:host :port :password :db])
                 (assoc :backend :kvrocks))
        conn {:pool pool
              :spec spec}]
    (if skip-checks?
      conn
      (let [db (:db spec)
            pw (:password spec)
            ns-cmd ["NAMESPACE" "ADD" db db]]
        (when-not (contains? @kvrocks-namespaces_ db)
          (try
            (if (seq pw)
              (wcar conn
                    (car/auth (:password spec))
                    (car/redis-call ns-cmd))
              (wcar conn
                    :as-pipeline
                    (car/redis-call ns-cmd)))
            (swap! kvrocks-namespaces_ conj db)
            conn
            (catch Exception e
              (let [err (e/error {:potamic/err-type :potamic/db-err
                                  :potamic/err-fatal? true
                                  :potamic/err-msg (str "Can't make Kvrocks connection: "
                                                        "Can't connect or create Kvrocks "
                                                        "namespace: " (.getMessage e))
                                  :potamic/err-data {::err (Throwable->map e)}})]
                (log/error err)
                (throw e)))))))
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
  [& {:keys [backend uri ?pool skip-checks?] :as args}]
  (if-let [args-err (v/invalidate dbv/MakeConnArgs args)]
    (let [err (e/error {:potamic/err-type :potamic/args-err
                        :potamic/err-fatal? true
                        :potamic/err-msg (str "Invalid args provided to "
                                              "potamic.db/make-conn")
                        :potamic/err-data {:args args :err args-err}})]
      (e/throw-potamic-error err))
    (let [pool (or ?pool (conn/make-connection-pool))]
      (case backend
        :kvrocks (-make-kvrocks-conn uri pool skip-checks?)
        (-make-redis-conn uri pool skip-checks?)))))

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
  (> (pcar conn (car/exists (pu/->str k))) 0))
