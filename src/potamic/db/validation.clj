(ns potamic.db.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]))

(def OptionalBackend
  [:backend {:optional true :default :redis} [:maybe [:enum :redis :kvrocks]]])

(def UriSpec
  [:map {:closed true}
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
   OptionalBackend])

(def MapSpec
  [:map {:closed true}
   [:host [:string {:min 1 :max 1024}]]
   [:port [:int {:min 1 :max 65535}]]
   [:user {:optional true} [:string {:min 1 :max 1024}]]
   [:password {:optional true} [:string {:min 1 :max 1024}]]
   [:db [:int {:min 0 :max 64}]]
   OptionalBackend])

(def Conn
  (malli/schema
    [:map
     [:spec [:or UriSpec MapSpec]]
     [:pool {:optional true} map?]]))

(def MakeConnArgs
  [:map
   OptionalBackend
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
   [:pool {:optional true} map?]
   [:skip-checks? {:optional true} [:maybe :boolean]]])
