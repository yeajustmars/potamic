(ns potamic.db.validation-test
  (:require [malli.core :as malli]
            [potamic.db.validation :as dbv]))

"

(def OptionalBackend
  [:backend {:optional true :default :redis} [:maybe [:enum :redis :kvrocks]]])

(def Conn
(def MakeConnArgs
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
"


(deftest test-OptionalBackend
  (testing "potamic.db.validation/OptionalBackend"
    (let [valid [

                 ]])
    ))

(deftest test-Conn
  (testing "potamic.db.validation/Conn"
    ))

(deftest test-MakeConnArgs
  (testing "potamic.db.validation/MakeConnArgs"
    ))
