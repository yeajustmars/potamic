(ns potamic.connection
  "Wraps `taoensso.carmine` connections for custom data backends (e.g. Kvrocks)."
  (:require [taoensso.encore :as enc]
            [taoensso.carmine :as car]
            [taoensso.carmine
             (protocol    :as protocol)
             (connections :as conns)]))

(defn make-connection-pool
  "Makes an Apache Commons Pool 2 connection pool based on CPU cores.

  Defaults to:

  - max-total = CPU cores * 8
  - max-idle = max-total * 0.75
  - min-idle = CPU cores"
  [& {:keys [max-total-multiplier max-idle-multiplier min-idle]
    :or {max-total-multiplier 8
         max-idle-multiplier 0.75
         min-idle nil}}]
  (let [cpu-cores (.availableProcessors (Runtime/getRuntime))
        max-total (* cpu-cores max-total-multiplier)
        max-idle  (* max-total max-idle-multiplier)
        min-idle  (or min-idle cpu-cores)]
    (car/connection-pool
      {:max-total-per-key max-total
       :max-idle-per-key  max-idle
       :min-idle-per-key  min-idle})))

#_:clj-kondo/ignore
(enc/defalias parse protocol/parse)
#_:clj-kondo/ignore
(enc/defalias return protocol/return)

(defmacro pcar
  "Implements `taoensso.carmine/wcar` supporting both Redis and Kvrocks backends."
  [conn-opts & args]
  (let [pipeline? (= :as-pipeline (first args))
        cmds      (if pipeline? (rest args) args)
        cmd-count (count cmds)
        g-pool    (gensym "pool")
        g-conn    (gensym "conn")
        g-backend (gensym "backend")
        g-db      (gensym "db")
        g-raw     (gensym "raw")
        g-trimmed (gensym "trimmed")
        args*     (if pipeline?
                    `(:as-pipeline (car/auth (str ~g-db)) ~@cmds)
                    `((car/auth (str ~g-db)) ~@cmds))]
    `(let [[~g-pool ~g-conn] (conns/pooled-conn ~conn-opts)
           ~g-backend (get-in ~g-conn [:spec :backend])
           ~g-db (or (get-in ~g-conn [:spec :db]) 0)
           ?stashed-replies# (when protocol/*context*
                               (protocol/execute-requests :get-replies :as-pipeline))]
       (try
         (let [response# (if (= ~g-backend :kvrocks)
                           (let [~g-raw (protocol/with-context ~g-conn
                                          (protocol/with-replies ~@args*))]
                             (if (vector? ~g-raw)
                               (let [~g-trimmed (subvec ~g-raw 1)]
                                 ~(if (and (not pipeline?) (= cmd-count 1))
                                    `(first ~g-trimmed)
                                    g-trimmed))
                               ~g-raw))
                           (protocol/with-context ~g-conn
                             (protocol/with-replies ~@args)))]
           (conns/release-conn ~g-pool ~g-conn)
           response#)
         (catch Throwable t#
           (conns/release-conn ~g-pool ~g-conn t#)
           (throw t#))
         (finally
           (when ?stashed-replies#
             (parse nil (enc/run! return ?stashed-replies#))))))))
