(ns potamic.db-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [potamic.db :as db]
            [potamic.util :as util]
            [potamic.test-util :as tu]))

(t/use-fixtures :each
                tu/fx-make-conns
                tu/fx-prime-db)

(deftest make-conn-test
  (letfn [(-test-redis-conns [uris & {:keys [backend skip-checks?]}]
            (doseq [uri uris]
              (let [?conn (if backend
                            (db/make-conn {:backend :redis :uri uri :skip-checks? skip-checks?})
                            (db/make-conn :uri uri :skip-checks? skip-checks?))]
                (is (= {:spec {:uri uri :backend :redis}
                        :pool :__REPLACED__}
                       (assoc ?conn :pool :__REPLACED__))))))
          (-test-kvrocks-conns [uris & {:keys [skip-checks?]}]
            (doseq [uri uris]
              (let [?conn (db/make-conn {:backend :kvrocks :uri uri :skip-checks? skip-checks?})
              {:keys [host port password db]} (util/parse-redis-uri uri)]
                (is (= {:spec {:backend :kvrocks
                               :host host
                               :port port
                               :password password
                               :db db}
                        :pool :__REPLACED__}
                       (assoc ?conn :pool :__REPLACED__))))))]
    (testing "| Redis"
      (testing "potamic.db/make-conn | Redis standalone (default, no explicit backend) | :skip-checks? true"
        (-test-redis-conns tu/valid-redis-uris :skip-checks? true))
      (testing "potamic.db/make-conn | Redis standalone (default, no explicit backend) | :skip-checks? false"
        (-test-redis-conns tu/testable-redis-uris))
      (testing "potamic.db/make-conn | Redis standalone (explicit backend) | :skip-checks? true"
        (-test-redis-conns tu/valid-redis-uris :backend :redis :skip-checks? true))
      (testing "potamic.db/make-conn | Redis standalone (explicit backend) | :skip-checks? false"
        (-test-redis-conns tu/valid-redis-uris :backend :redis :skip-checks? true)))
    (testing "| Kvrocks"
      (testing "potamic.db/make-conn | Kvrocks standalone (explicit backend) | :skip-checks? true"
        (-test-kvrocks-conns tu/valid-kvrocks-standalone-uris :backend :kvrocks :skip-checks? true))
      (testing "potamic.db/make-conn | Kvrocks standalone (explicit backend) | :skip-checks? false"
        (-test-kvrocks-conns tu/valid-kvrocks-standalone-uris :backend :kvrocks :skip-checks? true)))))

#_(deftest key-exists?-test
  (testing "potamic.db/make-conn | Redis standalone"
    )
  (testing "potamic.db/make-conn | Kvrocks standalone"
    )
  (testing "potamic.db/make-conn | Kvrocks cluster"
    ))
