(ns potamic.queue-test
  "Test `potamic.queue`."
  {:added "0.1"
   :author "@yeajustmars"}
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.walk :as walk]
            [clojure.core.async :as async]
            [taoensso.carmine :as car]
            [potamic.connection :refer [pcar]]
            [potamic.queue :as q]
            [potamic.test-util :as tu]))

(use-fixtures :each
                tu/fx-make-conns
                tu/fx-prime-db)

(deftest test__create-queue!
  (letfn [(-create-queue [conn]
            (let [[status ?err] (q/create-queue! tu/secondary-queue conn)]
              (is (= :created-with-new-stream status))
              (is (nil? ?err))))]
  (testing "potamic.queue/create-queue! | Redis standalone"
    (-create-queue tu/conn-redis-standalone)
    (tu/reset-queues!))
  (testing "potamic.queue/create-queue! | Kvrocks standalone"
    (-create-queue tu/conn-kvrocks-standalone)
    (tu/reset-queues!))
  (testing "potamic.queue/create-queue! | Kvrocks cluster"
    (-create-queue tu/conn-kvrocks-cluster)
    (tu/reset-queues!))))

(def ^:private correct-get-queues-redis-standalone
  {:my/test-queue {:group-name :my/test-queue-group,
                   :queue-conn {:spec {:backend :redis,
                                       :uri "redis://default:secret@localhost:6379/0"}},
                   :queue-name :my/test-queue,
                   :redis-group-name "my/test-queue-group",
                   :redis-queue-name "my/test-queue"},
   :secondary/queue {:group-name :second/group,
                     :queue-conn {:spec {:backend :redis,
                                         :uri "redis://default:secret@localhost:6379/0"}},
                     :queue-name :secondary/queue,
                     :redis-group-name "second/group",
                     :redis-queue-name "secondary/queue"}})

(def ^:private correct-get-queues-kvrocks-standalone
  {:my/test-queue {:group-name :my/test-queue-group,
                   :queue-conn {:spec {:backend :kvrocks,
                                       :db 0,
                                       :host "localhost",
                                       :password "secret",
                                       :port 6666}},
                   :queue-name :my/test-queue,
                   :redis-group-name "my/test-queue-group",
                   :redis-queue-name "my/test-queue"},
   :secondary/queue {:group-name :second/group,
                     :queue-conn {:spec {:backend :kvrocks,
                                         :db 0,
                                         :host "localhost",
                                         :password "secret",
                                         :port 6666}},
                     :queue-name :secondary/queue,
                     :redis-group-name "second/group",
                     :redis-queue-name "secondary/queue"}})

(def ^:private correct-get-queues-kvrocks-cluster
  {:my/test-queue {:group-name :my/test-queue-group,
                   :queue-conn {:spec {:backend :kvrocks,
                                       :db 0,
                                       :host "localhost",
                                       :password "secret",
                                       :port 6669}},
                   :queue-name :my/test-queue,
                   :redis-group-name "my/test-queue-group",
                   :redis-queue-name "my/test-queue"},
   :secondary/queue {:group-name :second/group,
                     :queue-conn {:spec {:backend :kvrocks,
                                         :db 0,
                                         :host "localhost",
                                         :password "secret",
                                         :port 6669}},
                     :queue-name :secondary/queue,
                     :redis-group-name "second/group",
                     :redis-queue-name "secondary/queue"}})

(deftest test__get-queues
  (testing "potamic.queue/get-queues"
    (letfn [(-test-queues [typ {{:keys [backend]} :spec :as conn}]
              (let [[_ ?err-destroy1] (q/destroy-queue! tu/test-queue conn :unsafe true)
                    [_ ?err-destroy2] (q/destroy-queue! tu/secondary-queue conn :unsafe true)
                    _ (tu/reset-queues!)
                    [status1 ?err1] (q/create-queue! tu/test-queue conn :group tu/test-queue-group)
                    [status2 ?err2] (q/create-queue! tu/secondary-queue conn :group tu/secondary-queue-group)
                    queues (walk/postwalk (fn [x] (if (map? x) (dissoc x :pool) x))
                                          (q/get-queues))]
                (is (nil? ?err-destroy1))
                (is (nil? ?err-destroy2))
                (is (= :created-with-new-stream status1))
                (is (= :created-with-new-stream status2))
                (is (nil? ?err1))
                (is (nil? ?err2))
                (is (= (case [backend typ]
                         [:redis :standalone] correct-get-queues-redis-standalone
                         [:kvrocks :standalone] correct-get-queues-kvrocks-standalone
                         [:kvrocks :cluster] correct-get-queues-kvrocks-cluster)
                       queues))))]
      (testing "potamic.queue/create-queue! | Redis standalone"
        (-test-queues :standalone tu/conn-redis-standalone))
      (testing "potamic.queue/test-queues! | Kvrocks standalone"
        (-test-queues :standalone tu/conn-kvrocks-standalone))
      (testing "potamic.queue/test-queues! | Kvrocks cluster"
        (-test-queues :cluster tu/conn-kvrocks-cluster)))))

(def ^:private correct-get-queue-redis-standalone
  {:my/test-queue {:group-name :my/test-queue-group,
                   :queue-conn {:spec {:backend :redis,
                                       :uri "redis://default:secret@localhost:6379/0"}},
                   :queue-name :my/test-queue,
                   :redis-group-name "my/test-queue-group",
                   :redis-queue-name "my/test-queue"}})

(def ^:private correct-get-queue-kvrocks-standalone
  {:my/test-queue {:group-name :my/test-queue-group,
                   :queue-conn {:spec {:backend :kvrocks,
                                       :db 0,
                                       :host "localhost",
                                       :password "secret",
                                       :port 6666}},
                   :queue-name :my/test-queue,
                   :redis-group-name "my/test-queue-group",
                   :redis-queue-name "my/test-queue"}})

(def ^:private correct-get-queue-kvrocks-cluster
  {:my/test-queue {:group-name :my/test-queue-group,
                   :queue-conn {:spec {:backend :kvrocks,
                                       :db 0,
                                       :host "localhost",
                                       :password "secret",
                                       :port 6669}},
                   :queue-name :my/test-queue,
                   :redis-group-name "my/test-queue-group",
                   :redis-queue-name "my/test-queue"}})

(deftest test__get-queue
  (testing "potamic.queue/get-queue"
    (letfn [(-test-queue [typ {{:keys [backend]} :spec :as conn}]
              (let [[_ ?err-destroy1] (q/destroy-queue! tu/test-queue conn :unsafe true)
                    _ (tu/reset-queues!)
                    [status1 ?err1] (q/create-queue! tu/test-queue conn :group tu/test-queue-group)
                    queue (walk/postwalk (fn [x] (if (map? x) (dissoc x :pool) x))
                                         (q/get-queues))]
                (is (nil? ?err-destroy1))
                (is (= :created-with-new-stream status1))
                (is (nil? ?err1))
                (is (= (case [backend typ]
                         [:redis :standalone] correct-get-queue-redis-standalone
                         [:kvrocks :standalone] correct-get-queue-kvrocks-standalone
                         [:kvrocks :cluster] correct-get-queue-kvrocks-cluster)
                       queue))))]
      (testing "potamic.queue/create-queue! | Redis standalone"
        (-test-queue :standalone tu/conn-redis-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks standalone"
        (-test-queue :standalone tu/conn-kvrocks-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks cluster"
        (-test-queue :cluster tu/conn-kvrocks-cluster)))))

(deftest test__put
  (testing "potamic.queue/put"
    (letfn [(-test-put [typ {{:keys [backend]} :spec :as conn}]
              (testing (str "| " backend " " typ)
                (let [qname (keyword (name backend) (str (name typ) "-put-test"))
                      [_ ?destroy-err] (q/destroy-queue! qname conn :unsafe true)
                      [create-status ?create-err] (q/create-queue! qname conn)]
                  (testing "-- queue creation"
                    (is (nil? ?destroy-err))
                    (is (= :created-with-new-stream create-status))
                    (is (nil? ?create-err)))
                  (testing "-- singular put (auto-id)"
                    (let [[?ids ?err] (q/put qname {:a 1})]
                      (is (nil? ?err))
                      (is (= (count ?ids) 1))
                      (is (re-find tu/id-pat (first ?ids)))))
                  (testing "-- multi put"
                    (let [[?ids ?err] (q/put qname {:a 1} {:b 2} {:c 3})]
                      (is (nil? ?err))
                      (is (= (count ?ids) 3))
                      (is (every? identity (mapv #(re-find tu/id-pat %) ?ids))))))))]
      (testing "potamic.queue/create-queue! | Redis standalone"
        (-test-put :standalone tu/conn-redis-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks standalone"
        (-test-put :standalone tu/conn-kvrocks-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks cluster"
        (-test-put :cluster tu/conn-kvrocks-cluster)))))

(deftest test__read
  (testing "potamic.queue/read"
    (letfn [(-test-read [typ {{:keys [backend]} :spec :as conn}]
              (let [
                    qname (keyword (name backend) (str (name typ) "-read-test"))
                    [_ ?destroy-err] (q/destroy-queue! qname conn :unsafe true)
                    [_ ?create-err] (q/create-queue! qname conn)
                    [_ _] (q/put qname {:a 1} {:b 2} {:c 3})
                    [read1-msgs ?read1-err] (q/read qname)
                    [read2-msgs ?read2-err] (q/read qname :start 0)
                    _ (q/put qname {:d 4} {:e 5} {:f 6})
                    [read3-msgs ?read3-err] (q/read qname
                                                    :count 1
                                                    :start (:id (last read1-msgs))
                                                    :block [300 :millis])]
                (is (nil? ?destroy-err))
                (is (nil? ?create-err))
                (is (nil? ?read1-err))
                (is (nil? ?read2-err))
                (is (nil? ?read3-err))
                (is (every? #(re-find tu/id-pat %) (map :id read1-msgs)))
                (is (every? #(re-find tu/id-pat %) (map :id read2-msgs)))
                (is (= read1-msgs read2-msgs))
                (is (re-find tu/id-pat (:id (first read3-msgs))))
                (is (= 1 (count read3-msgs)))
                (is (= (:msg (first read3-msgs)) {:d 4}))))]
      (testing "potamic.queue/create-queue! | Redis standalone"
        (-test-read :standalone tu/conn-redis-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks standalone"
        (-test-read :standalone tu/conn-kvrocks-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks cluster"
        (-test-read :cluster tu/conn-kvrocks-cluster)))))

(deftest test__read-next!
  (testing "potamic.queue/read-next!"
    (letfn [(-test-read-next! [typ {{:keys [backend]} :spec :as conn}]
              (let [qname (keyword (name backend) (str (name typ) "-read-next!-test"))
                    [_ ?destroy-err] (q/destroy-queue! qname conn :unsafe true)
                    [_ ?create-err] (q/create-queue! qname conn)
                    [?ids ?err] (q/put qname {:a 1} {:b 2} {:c 3})]
                (is (nil? ?destroy-err))
                (is (nil? ?err))
                (is (nil? ?create-err))
                (is (= (count ?ids) 3))
                (is (every? identity (mapv #(re-find tu/id-pat %) ?ids)))
                (testing "-- read-next! 1"
                  (let [[?msgs ?e] (q/read-next! 1 :from qname :as :my/consumer1)]
                    (is (nil? ?e))
                    (is (= 1 (count ?msgs)))
                    (is (re-find tu/id-pat (:id (first ?msgs))))
                    (is (= (:msg (first ?msgs)) {:a 1}))))
                (testing "-- read-next! :all"
                  (let [[?msgs ?e] (q/read-next! 2 :from qname :as :my/consumer1)]
                    (is (nil? ?e))
                    (is (= 2 (count ?msgs)))
                    (is (re-find tu/id-pat (:id (first ?msgs))))
                    (is (re-find tu/id-pat (:id (second ?msgs))))
                    (is (= (:msg (first ?msgs)) {:b 2}))
                    (is (= (:msg (second ?msgs)) {:c 3}))))))]
      (testing "potamic.queue/create-queue! | Redis standalone"
        (-test-read-next! :standalone tu/conn-redis-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks standalone"
        (-test-read-next! :standalone tu/conn-kvrocks-standalone))
      (testing "potamic.queue/test-queue! | Kvrocks cluster"
        (-test-read-next! :cluster tu/conn-kvrocks-cluster)))))

#_(deftest test__read-pending
  (testing "potamic.queue/read-pending"
    (let [[_ _] (q/put test-queue {:a 1} {:b 2} {:c 3})
          [_ _] (q/read-next! 1 :from test-queue :as :consumer/one)
          [read1 ?read1-err] (q/read-pending 10
                                             :from test-queue
                                             :for :consumer/one)
          [read2 ?read2-err ] (q/read-pending 10
                                              :from test-queue
                                              :for :consumer/one
                                              :start '-
                                              :end '+)
          [read3 ?read3-err] (q/read-pending 1
                                             :from test-queue
                                             :for :consumer/one
                                             :start (:id (first read1))
                                             :end  (:id (last read2)))]
      (is (nil? ?read1-err))
      (is (nil? ?read2-err))
      (is (nil? ?read3-err))
      (is (sequential? read1))
      (is (sequential? read2))
      (is (sequential? read3))
      (is (= (count read1) 1))
      (is (= (count read2) 1))
      (is (= (count read3) 1))
      (is (= (:id (first read1)) (:id (first read2))))
      (is (= (:id (first read2)) (:id (first read3))))
      (is (= (:id (first read1)) (:id (first read3)))))))

#_(deftest test__read-pending-summary
  (testing "potamic.queue/read-pending-summary"
    (let [qnm test-queue
          [msg-ids ?put-err]   (q/put qnm {:a 1} {:b 2} {:c 3})
          [c1-msgs ?read1-err] (q/read-next! 2 :from qnm :as :my/consumer1)
          [p1-summary ?p1-err] (q/read-pending-summary qnm)
          [c2-msgs ?read2-err] (q/read-next! 1 :from qnm :as :my/consumer2)
          [p2-summary ?p2-err] (q/read-pending-summary qnm)]
      (is (nil? ?put-err))
      (is (nil? ?read1-err))
      (is (nil? ?read2-err))
      (is (nil? ?p1-err))
      (is (nil? ?p2-err))
      (is (every? #(re-find tu/id-pat %) msg-ids))
      (is (every? #(re-find tu/id-pat %) (map :id c1-msgs)))
      (is (= (:msg (first c1-msgs)) {:a 1}))
      (is (= (:msg (second c1-msgs)) {:b 2}))
      (is (every? #(re-find tu/id-pat %) (map :id c2-msgs)))
      (is (= (:msg (first c2-msgs)) {:c 3}))
      (is (= (:total p1-summary) 2))
      (is (re-find tu/id-pat (:start p1-summary)))
      (is (re-find tu/id-pat (:end p1-summary)))
      (is (= (:consumers p1-summary) {:my/consumer1 2}))
      (is (= (:total p2-summary) 3))
      (is (re-find tu/id-pat (:start p2-summary)))
      (is (re-find tu/id-pat (:end p2-summary)))
      (is (= (:consumers p2-summary) {:my/consumer1 2, :my/consumer2 1})))))

#_(deftest test__read-range
  (testing "potamic.queue/read-range"
    (let [[_ _] (q/put test-queue {:a 1} {:b 2} {:c 3})
          [r1 ?e1] (q/read-range test-queue :start '- :end '+)
          [r2 ?e2] (q/read-range test-queue :start '- :end '+ :count 10)]
      (is (nil? ?e1))
      (is (nil? ?e2))
      (is (every? #(re-find tu/id-pat %) (map :id r1)))
      (is (every? #(re-find tu/id-pat %) (map :id r2)))
      (is (= (:msg (first r1)) {:a 1}))
      (is (= (:msg (second r1)) {:b 2}))
      (is (= (:msg (nth r1 2)) {:c 3}))
      (is (= (:msg (first r2)) {:a 1}))
      (is (= (:msg (second r2)) {:b 2}))
      (is (= (:msg (nth r2 2)) {:c 3}))
      (is (= (:msg (first r1)) (:msg (first r2)))))))

#_(deftest test__set-processed!
  (testing "potamic.queue/set-processed!"
    (let [_ (q/put test-queue {:a 1} {:b 2} {:c 3})
          [msgs ?read-err] (q/read-next! 3
                                         :from test-queue
                                         :as test-queue-group)
          ids (map :id msgs)
          [n-acked ?ack-err] (apply q/set-processed! test-queue ids)]
      (is (nil? ?read-err))
      (is (nil? ?ack-err))
      (is (= 3 n-acked)))))

#_(deftest test__delete-queue
  (testing "potamic.queue/delete-queue"
    (let [[_ ?put-err] (q/put test-queue {:a 1} {:b 2} {:c 3})
          [_ ?read-err] (q/read-next! 2 :from test-queue :as :c/one)
          [nil-response safe-block-err] (q/destroy-queue! test-queue conn)
          [destroyed-status ?destroy-err] (q/destroy-queue! test-queue
                                                            conn
                                                            :unsafe true)
          [nonexistent-status ?nonexistent-err] (q/destroy-queue! test-queue
                                                                  conn)]
      (is (nil? ?put-err))
      (is (nil? ?read-err))
      (is (nil? nil-response))
      (is (= :potamic/db-err
             (:potamic/err-type safe-block-err)))
      (is (re-find #"Cannot\s+destroy.+?,\s+it has pending messages"
                   (:potamic/err-msg safe-block-err)))
      (is (nil? ?destroy-err))
      (is (= :spec-destroyed_stream-destroyed destroyed-status))
      (is (nil? ?nonexistent-err))
      (is (= :spec-nonexistent_stream-nonexistent nonexistent-status)))))

#_(deftest test__create-destroy-cycle
  (testing "creating > destroying > creating cycle"
    (q/destroy-queue! test-queue conn :unsafe true)
    (pcar conn (car/flushall))
    (let [[create1-res ?create1-err] (q/create-queue! test-queue conn)
          [destroy1-res ?destroy1-err] (q/destroy-queue! test-queue conn)
          [create2-res ?create2-err] (q/create-queue! test-queue conn)
          [destroy2-res ?destroy2-err] (q/destroy-queue! test-queue conn)
          [create3-res ?create3-err] (q/create-queue! test-queue conn)
          [create4-res ?create4-err] (q/create-queue! test-queue conn)]
      (is (= :created-with-new-stream create1-res))
      (is (nil? ?create1-err))
      (is (= :spec-destroyed_stream-destroyed destroy1-res))
      (is (nil? ?destroy1-err))
      (is (= :created-with-new-stream create2-res))
      (is (nil? ?create2-err))
      (is (= :spec-destroyed_stream-destroyed destroy2-res))
      (is (nil? ?destroy2-err))
      (is (= :created-with-new-stream create3-res))
      (is (nil? ?create3-err))
      (is (= :updated-with-existing-stream create4-res))
      (is (nil? ?create4-err)))))
