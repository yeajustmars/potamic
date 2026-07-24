(ns potamic.db.validation-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [malli.core :as malli]
            [potamic.connection :as conn]
            [potamic.db.validation :as dbv]
            [potamic.test-util :as tu]))

(deftest test-OptionalBackend
  (testing "potamic.db.validation/OptionalBackend"
    (let [valids [{}
                 {:backend nil}
                 {:backend :redis}
                 {:backend :kvrocks}]]
      (doseq [in valids]
        (is (true? (malli/validate [:map dbv/OptionalBackend] in)))))))

(deftest test-Conn
  (testing "potamic.db.validation/Conn"
    (let [valids (into (mapv (fn [redis-uri] {:spec {:uri redis-uri}})
                             tu/valid-redis-uris)
                       [{:spec {:uri "redis://default:secret@localhost:6379/0"
                                :backend :redis}}
                        {:spec {:uri "redis://default:secret@localhost:6379/0"
                                :backend :kvrocks}}])
          invalids [{}
                    {:spec {}}
                    {:spec {:uri "__INVALID-URI__"}}
                    {:spec {:uri "__INVALID-URI__" :backend :redis}}
                    {:spec {:uri "__INVALID-URI__" :backend :kvrocks}}
                    {:spec {:uri "redis://default:secret@localhost:6379/0"
                            :backend :kvrocks
                            :some/extra-key "__NOT-ALLOWED__"}}]]
      (testing "| valid input"
        (doseq [in valids]
          (is (true? (malli/validate dbv/Conn in)))))
      (testing "| invalid input"
        (doseq [in invalids]
          (is (false? (malli/validate dbv/Conn in))))))))

(deftest test-MakeConnArgs
   (testing "potamic.db.validation/MakeConnArgs"
     (let [custom-pool (conn/make-connection-pool)
           valids (into (mapv (fn [redis-uri] {:uri redis-uri})
                              tu/valid-redis-uris)
                        [{:uri "redis://default:secret@localhost:6379/0"
                          :backend :redis}
                         {:uri "redis://default:secret@localhost:6379/1"
                          :backend :kvrocks}
                         {:uri "redis://default:secret@localhost:6379/0"
                          :backend :redis
                          :skip-checks? true}
                         {:uri "redis://default:secret@localhost:6379/1"
                          :backend :kvrocks
                          :skip-checks? false}
                         {:uri "redis://default:secret@localhost:6379/1"
                          :backend :kvrocks
                          :skip-checks? nil}
                         {:uri "redis://default:secret@localhost:6379/2"
                          :backend :redis
                          :pool custom-pool}
                         {:uri "redis://default:secret@localhost:6379/15"
                          :backend :kvrocks
                          :pool custom-pool}
                         {:uri "redis://default:secret@localhost:6379/2"
                          :backend :redis
                          :pool custom-pool
                          :skip-checks? true}
                         {:uri "redis://default:secret@localhost:6379/15"
                          :backend :kvrocks
                          :pool custom-pool
                          :skip-checks? false}
                         {:uri "redis://default:secret@localhost:6379/15"
                          :backend :kvrocks
                          :pool custom-pool
                          :skip-checks? nil}])
           invalids [{}
                     {:missing/uri "__NOT-ALLOWED__"}
                     {:uri "redis://default:secret@localhost:6379/0"
                      :backend :redis
                      :skip-checks? 123}
                     {:uri "redis://default:secret@localhost:6379/1"
                      :backend :kvrocks
                      :skip-checks? "__STRING__"}
                     {:uri "redis://default:secret@localhost:6379/0"
                      :backend :redis
                      :pool 123}
                     {:uri "redis://default:secret@localhost:6379/1"
                      :backend :kvrocks
                      :pool "__STRING__"}]]
      (testing "| valid input"
        (doseq [in valids]
          (is (true? (malli/validate dbv/MakeConnArgs in)))))
      (testing "| invalid input"
        (doseq [in invalids]
          (is (false? (malli/validate dbv/MakeConnArgs in))))))))
