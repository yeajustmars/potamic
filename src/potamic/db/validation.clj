(ns potamic.db.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]))

(def OptionalBackend
  [:backend {:optional true :default :redis} [:maybe [:enum :redis :kvrocks]]])

(def OptionalKvrocksMode
  [:kvrocks-mode {:optional true :default :namespace} [:maybe [:enum :redis :namespace]]])

(def UriSpec
  [:map {:closed true}
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
   OptionalBackend
   OptionalKvrocksMode])

(def MapSpec
  [:map {:closed true}
   [:host [:string {:min 1 :max 1024}]]
   [:port [:int {:min 1 :max 65535}]]
   [:user {:optional true} [:maybe [:string {:min 1 :max 1024}]]]
   [:password {:optional true} [:maybe [:string {:min 1 :max 1024}]]]
   [:db [:int {:min 0 :max 64}]]
   OptionalBackend
   OptionalKvrocksMode])

(def Conn
  (malli/schema
    [:map
     [:spec [:or UriSpec MapSpec]]
     [:pool {:optional true} map?]]))

(def MakeConnArgs
  [:map
   OptionalBackend
   OptionalKvrocksMode
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
   [:pool {:optional true} map?]
   [:skip-checks? {:optional true} [:maybe :boolean]]])
