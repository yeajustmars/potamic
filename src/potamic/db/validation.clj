(ns potamic.db.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v])
  (:gen-class))

(def OptionalBackend
  [:backend {:optional true :default :redis} [:enum :redis :kvrocks]])

(def Conn
  (malli/schema
    [:map {:closed true}
     OptionalBackend
     [:spec [:map {:closed true} [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]]]
     [:pool {:optional true} map?]]))

(def ConnArgs
  [:map {:closed true}
   OptionalBackend
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
   [:pool {:optional true} map?]])
