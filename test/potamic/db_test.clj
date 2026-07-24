(ns potamic.db-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [potamic.connection :refer [pcar]]
            [potamic.db :as db]
            [potamic.util :as util]
            [potamic.test-util :as tu]
            [taoensso.carmine :as car])
  (:import [java.util UUID]))

(t/use-fixtures :each
                tu/fx-make-conns
                tu/fx-prime-db)

(deftest test-make-conn
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
      (testing "potamic.db/make-conn | Kvrocks standalone | :skip-checks? true"
        (-test-kvrocks-conns tu/valid-kvrocks-standalone-uris :backend :kvrocks :skip-checks? true))

      (testing "potamic.db/make-conn | Kvrocks standalone | :skip-checks? false"
        (-test-kvrocks-conns tu/valid-kvrocks-standalone-uris :backend :kvrocks :skip-checks? true))

      (testing "potamic.db/make-conn | Kvrocks cluster | :skip-checks? true"
        (-test-kvrocks-conns tu/valid-kvrocks-cluster-uris :backend :kvrocks :skip-checks? true))

      (testing "potamic.db/make-conn | Kvrocks cluster | :skip-checks? false"
        (-test-kvrocks-conns tu/valid-kvrocks-cluster-uris :backend :kvrocks :skip-checks? true)))))

(deftest test-key-exists?
  (let [random-key (str (UUID/randomUUID))]
    (letfn [(-set-x-to-one [conn] (pcar conn (car/set random-key 1)))]
      (testing "potamic.db/key-exists? | Redis standalone"
        (let [exists-before? (db/key-exists? random-key tu/conn-redis-standalone)
              _ (-set-x-to-one tu/conn-redis-standalone)
              exists-after? (db/key-exists? random-key tu/conn-redis-standalone)]
          (is (false? exists-before?))
          (is (true? exists-after?))))

      (testing "potamic.db/key-exists? | Kvrocks standalone"
        (let [exists-before? (db/key-exists? random-key tu/conn-kvrocks-standalone)
              _ (-set-x-to-one tu/conn-kvrocks-standalone)
              exists-after? (db/key-exists? random-key tu/conn-kvrocks-standalone)]
          (is (false? exists-before?))
          (is (true? exists-after?))))

      (testing "potamic.db/key-exists? | Kvrocks cluster"
        (let [exists-before? (db/key-exists? random-key tu/conn-kvrocks-cluster)
              _ (-set-x-to-one tu/conn-kvrocks-cluster)
              exists-after? (db/key-exists? random-key tu/conn-kvrocks-cluster)]
          (is (false? exists-before?))
          (is (true? exists-after?)))))))
