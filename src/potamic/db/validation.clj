(ns potamic.db.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]))

(def OptionalBackend
  [:backend {:optional true :default :redis} [:maybe [:enum :redis :kvrocks]]])

(def Conn
  (malli/schema
    [:map
     [:spec [:map {:closed true}
             [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
             OptionalBackend]]
     [:pool {:optional true} map?]]))

(def MakeConnArgs
  [:map
   OptionalBackend
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
   [:pool {:optional true} map?]
   [:skip-checks? {:optional true} [:maybe :boolean]]])
