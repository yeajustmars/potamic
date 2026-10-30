(ns potamic.validation
  (:require
    [malli.core :as m]
    [malli.error :as me]))

(def re-redis-uri #"(?i)^(redis|kvrocks)://.*")

(defn valid-redis-uri?
  [value]
  (boolean (re-find re-redis-uri (str value))))

(defmacro f
  [validation-fn err-msg]
  `[:fn {:error/message ~err-msg} ~validation-fn])

(defn invalidate
  [schema value]
  (when-let [e (m/explain schema value)]
    (me/humanize e)))
