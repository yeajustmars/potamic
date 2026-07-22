(ns potamic.db-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.db :as db]
            [potamic.test-data :refer [valid-redis-uris]]))

(deftest make-conn-test
  (testing "potamic.db/make-conn"
    (doseq [uri valid-redis-uris
            :let [?conn (db/make-conn :uri uri)]]
      (is (= {:spec {:uri uri :backend :redis} :pool {}}
             (assoc ?conn :pool {}))))))
