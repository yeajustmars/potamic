(ns potamic.util
  "Common utilities."
  (:require [clojure.string :as string]
            [com.rpl.specter :as s]
            [taoensso.nippy :as nippy])
  (:import [java.net URI]))

(defn ->str
  "Returns a string representation of symbol. This is similar to calling `str`
  on a symbol except that keywords will not contain a preceding colon
  character. The keyword `:x/y` will yield \"x/y\" instead of \":x/y\"."
  [x]
  (cond
    (string? x) x
    (keyword? x) (subs (str x) 1)
    :else (str x)))

(defn <-str
  "Returns keyword for `x` if `x` is a non-empty string and doesn't start with a number,
  else returns `x` as-is.

  _NOTE_: Redis Stream IDs won't be coerced to keywords."
  [x]
  (if-not (string? x)
    x
    (if (and (seq x) (Character/isDigit (first x)))
      x
      (keyword x))))

(defn ->int
  "Returns string `s` parsed to integer."
  [s]
  (Integer/parseInt s))

(defn prep-cmd
  "Returns concatenated vector of arguments (without using concat) to be
  applied to a Carmine/Redis command."
  [args]
  (->> args
       (flatten)
       (remove nil?)
       (s/transform [s/ALL #(or (symbol? %) (keyword? %))] ->str)))

(defn time->milliseconds
  "Converts high-level time syntax (e.g. `[5 :seconds]`) to milliseconds.
  If `t` is an integer, or if `:milli`/`millis` is provided as `interval`,
  it is returned as-is. Returns an integer for milliseconds."
  [t]
  (if (vector? t)
    (let [[n interval] t]
      (case interval
        (:milli :millis) n
        (:second :seconds) (* n 1000)
        (:minute :minutes) (* n 1000 60)
        (:hour :hours) (* n 1000 60 60)))
    t))

(defn remove-conn
  [x]
  (if (:conn x)
    (dissoc x :conn)
    x))

(defn uuid
  []
  (java.util.UUID/randomUUID))

(defn make-exception
  [e]
  (let [e* (Throwable->map e)]
    (update e* :trace #(subvec % 0 (min 10 (count %))))))

(def RECURSIVE-WALK
  "_(Implementation detail)_

  Compiled path for nested data structures."
  (s/comp-paths
    (s/recursive-path []
                      #_:clj-kondo/ignore p*
                      (s/cond-path
                        map? (s/continue-then-stay [s/MAP-VALS p*])
                        sequential? [s/ALL p*]
                        s/STAY))))

(defn encode-map-vals
  "Returns map after encoding map vals by calling `nippy/freeze` on them.
  Nested maps are encoded wholesale as Redis/KeyDB streams don't allow nesting."
  [m]
  (into {} (for [[k v] m] [k (nippy/freeze v)])))

(defn parse-redis-uri
  "Parses a Redis/Kvrocks URI string using java.net.URI into a Clojure map."
  [uri-str]
  (let [uri (URI. uri-str)
        user-info (.getUserInfo uri)
        [?user ?password] (when (seq user-info)
                            (if (string/includes? user-info ":")
                              (string/split user-info #":" 2)
                              [nil user-info]))
        ?user (when (seq ?user) ?user)
        ?password (when (seq ?password) ?password)
        path (.getPath uri)
        db (if (and (seq path) (re-find #"/[0-9]+" path))
             (try
               (Integer/parseInt (subs path 1))
               (catch Exception _ 0))
             0)
        query (.getQuery uri)
        query-params (when query
                       (into {}
                             (for [kv (string/split query #"&")
                                   :let [[k v] (string/split kv #"=" 2)]]
                               [k (or v "")])))]
    (cond-> {:scheme (.getScheme uri)
             :user ?user
             :password ?password
             :host (.getHost uri)
             :port (let [p (.getPort uri)] (when (pos? p) p))
             :db db}
      query-params (assoc :query query-params))))
