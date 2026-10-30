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
  [uri parsed-uri pool skip-checks?]
  (let [;; If it's a native redis:// URI, preserve the raw URI string for standard Carmine behavior.
        ;; If it's a kvrocks:// URI in Redis-mode, unpack it so Carmine doesn't reject the scheme.
        spec (if (= "redis" (:scheme parsed-uri))
               {:backend :redis :uri uri}
               (-> (select-keys parsed-uri [:host :port :password :db :user])
                   (assoc :backend :redis)))
        conn {:pool pool :spec spec}]
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
  [parsed-uri pool skip-checks?]
  (let [;; Explicitly extract ONLY host, port, password, and db.
        ;; This intentionally drops :user to avoid Carmine's Redis 6+ ACL AUTH bug with Kvrocks.
        spec (-> (select-keys parsed-uri [:host :port :password :db])
                 (assoc :backend :kvrocks))
        conn {:pool pool
              :spec spec}]
    (if skip-checks?
      conn
      (let [db (:db spec)
            pw (:password spec)
            ns-cmd ["NAMESPACE" "ADD" (str db) (str db)]]
        (when-not (contains? @kvrocks-namespaces_ db)
          (try
            (if (seq pw)
              (wcar conn
                    (car/auth pw)
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
  [& {:keys [backend kvrocks-mode uri pool skip-checks?] :as args}]
  (if-let [args-err (v/invalidate dbv/MakeConnArgs args)]
    (let [err (e/error {:potamic/err-type :potamic/args-err
                        :potamic/err-fatal? true
                        :potamic/err-msg (str "Invalid args provided to "
                                              "potamic.db/make-conn")
                        :potamic/err-data {:args args :err args-err}})]
      (e/throw-potamic-error err))
    (let [pool (or pool (conn/make-connection-pool))
          parsed-uri (pu/parse-redis-uri uri)
          scheme-backend (when (= "kvrocks" (:scheme parsed-uri)) :kvrocks)
          uri-mode (get-in parsed-uri [:query "mode"])
          mode (or kvrocks-mode (when (= uri-mode "redis") :redis) :namespace)

          resolved-backend (if (and (or (= scheme-backend :kvrocks) (= backend :kvrocks))
                                    (= mode :redis))
                             :redis
                             (or scheme-backend backend :redis))]
      (case resolved-backend
        :kvrocks (-make-kvrocks-conn parsed-uri pool skip-checks?)
        (-make-redis-conn uri parsed-uri pool skip-checks?)))))

(defn key-exists?
  "Returns boolean after checking if key exists in DB."
  [k conn]
  (> (pcar conn (car/exists (pu/->str k))) 0))
