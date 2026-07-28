(ns potamic.sentinel-test
  "Tests `potamic.sentinel`."
  {:added "0.1"
   :author "@yeajustmars"}
  (:require
    [clojure.core.async :as async :refer [<!!]]
    [clojure.test :as tests :refer (deftest is testing)]
    [potamic.connection :as conn :refer [pcar]]
    [potamic.db]
    [potamic.fmt :as fmt]
    [potamic.queue :as q]
    [potamic.queue.queues :as queues]
    [potamic.sentinel :as s]
    [potamic.test-util :as tu]
    [taoensso.carmine :as car]
    [taoensso.timbre :as log])
  (:import [taoensso.carmine.connections ConnectionPool]))

(tests/use-fixtures :each
                    tu/fx-make-conns
                    tu/fx-prime-db)

(def ^:const ONE-HUNDRED-MILLISECONDS 100)

(defn basic-counter-handler
  [this]
  (if (<= (s/get-attr this :n-runs) 1)
    (s/set-attr this :new-count 1)
    (let [cur-count (s/get-attr this :new-count)
          new-count (inc cur-count)]
      (-> this
          (s/set-attr :old-count cur-count)
          (s/set-attr :new-count new-count)))))

(defn- -new-redis-standalone-sentinel
  [queue-name queue-group]
  (s/create-sentinel {:queue-uri tu/uri-redis-standalone
                      :queue-name queue-name
                      :queue-group queue-group
                      :frequency ONE-HUNDRED-MILLISECONDS
                      :handler basic-counter-handler}))

(defn- -new-kvrocks-standalone-sentinel
  [queue-name queue-group]
  (s/create-sentinel {:queue-uri tu/uri-kvrocks-standalone
                      :queue-backend :kvrocks
                      :queue-name queue-name
                      :queue-group queue-group
                      :frequency ONE-HUNDRED-MILLISECONDS
                      :handler basic-counter-handler}))

(defn- -new-kvrocks-cluster-sentinel
  [queue-name queue-group]
  (s/create-sentinel {:queue-uri tu/uri-kvrocks-cluster
                      :queue-backend :kvrocks
                      :queue-name queue-name
                      :queue-group queue-group
                      :frequency ONE-HUNDRED-MILLISECONDS
                      :handler basic-counter-handler}))

(deftest test__create-sentinel
  (testing "potamic.sentinel/create-sentinel"
    (letfn [(-test-redis-standalone-sentinel []
              (let [queue-name 'redis/standalone-sentinel
                    queue-group 'redis/standalone-sentinel-group
                    s (-new-redis-standalone-sentinel queue-name queue-group)]
                (is (instance? ConnectionPool (get-in s [:queue-conn :pool])))
                (is (= tu/uri-redis-standalone (get-in s [:queue-conn :spec :uri])))
                (is (= queue-name (:queue-name s)))
                (is (= queue-group (:queue-group s)))
                (is (= 0 (:init-id s)))
                (is (= ONE-HUNDRED-MILLISECONDS (:frequency s)))
                (is (= 0 (:start-offset s)))
                (is (= {:started? false
                        :stopped? false
                        :n-runs 0}
                       (s/get-state s)))))
            (-test-kvrocks-standalone-sentinel []
              (let [queue-name :kvrocks/standalone-sentinel
                    queue-group :kvrocks/standalone-sentinel-group
                    s (-new-kvrocks-standalone-sentinel queue-name queue-group)]
                (is (instance? ConnectionPool (get-in s [:queue-conn :pool])))
                (is (= "localhost" (get-in s [:queue-conn :spec :host])))
                (is (= 6666 (get-in s [:queue-conn :spec :port])))
                (is (= 0 (get-in s [:queue-conn :spec :db])))
                (is (= "secret" (get-in s [:queue-conn :spec :password])))
                (is (= queue-name (:queue-name s)))
                (is (= queue-group (:queue-group s)))
                (is (= 0 (:init-id s)))
                (is (= ONE-HUNDRED-MILLISECONDS (:frequency s)))
                (is (= 0 (:start-offset s)))
                (is (= {:started? false
                        :stopped? false
                        :n-runs 0}
                       (s/get-state s)))))
            (-test-kvrocks-cluster-sentinel []
              (let [queue-name :kvrocks/cluster-sentinel
                    queue-group :kvrocks/cluster-sentinel-group
                    s (-new-kvrocks-cluster-sentinel queue-name queue-group)]
                (is (instance? ConnectionPool (get-in s [:queue-conn :pool])))
                (is (= "localhost" (get-in s [:queue-conn :spec :host])))
                (is (= 6669 (get-in s [:queue-conn :spec :port])))
                (is (= 0 (get-in s [:queue-conn :spec :db])))
                (is (= "secret" (get-in s [:queue-conn :spec :password])))
                (is (= queue-name (:queue-name s)))
                (is (= queue-name (s/get-queue-name s)))
                (is (= queue-group (:queue-group s)))
                (is (= queue-group (s/get-queue-group s)))
                (is (= 0 (:init-id s)))
                (is (= ONE-HUNDRED-MILLISECONDS (:frequency s)))
                (is (= 0 (:start-offset s)))
                (is (= {:started? false
                        :stopped? false
                        :n-runs 0}
                       (s/get-state s)))))]
      (testing "potamic.sentinel/create-sentinel | Redis standalone"
        (-test-redis-standalone-sentinel))
      (testing "potamic.sentinel/create-sentinel | Kvrocks standalone"
        (-test-kvrocks-standalone-sentinel))
      (testing "potamic.sentinel/create-sentinel | Kvrocks cluster"
        (-test-kvrocks-cluster-sentinel)))))

(defn- -test-sentinel-runtime
  [backend typ constructor]
  (testing (str "| Runtime | " backend " | " typ)
    (let [q-name (keyword (name backend) (name typ))
          q-group (keyword (name backend) (str (name typ) "-group"))
          s (constructor q-name q-group)]
      (is (satisfies? potamic.sentinel/SentinelProtocol s))
      (try
        (testing "| Start sentinel"
          (s/start-sentinel! s)
          (<!! (async/timeout (* 4 ONE-HUNDRED-MILLISECONDS)))
          (is (true? (s/get-attr s :started?)))
          (is (> (s/get-attr s :new-count) (s/get-attr s :old-count) )))
        (catch Exception e
          (log/error e))
        (finally
          (testing "| Stop sentinel"
            (s/stop-sentinel! s)
            (<!! (async/timeout (* 4 ONE-HUNDRED-MILLISECONDS)))
            (is (true? (s/get-attr s :stopped?)))
            (is (> (s/get-attr s :new-count) (s/get-attr s :old-count) ))))))))

(deftest test__redis-standalone-sentinel
  (-test-sentinel-runtime :redis :standalone -new-redis-standalone-sentinel))

(deftest test__kvrocks-standalone-sentinel
  (-test-sentinel-runtime :kvrocks :standalone -new-kvrocks-standalone-sentinel))

(deftest test__kvrocks-cluster-sentinel
  (-test-sentinel-runtime :kvrocks :cluster -new-kvrocks-cluster-sentinel))

#_(defn- -test-sentinel-producer-consumer-model
  [backend typ constructor]
  (testing (str "Pub/sub | " backend " | " typ)
    ))

;; (deftest sentinel-producer-consumer-test1
;;   (testing "queue read/write from within Sentinel"
;;     (let [s (basic-sentinel
;;               (fn [this]
;;                 (let [qname (s/get-queue-name this)
;;                       n-runs (s/get-attr this :n-runs)]
;;                   (if (= n-runs 2)
;;                     (s/stop-sentinel! this)
;;                     (q/put qname {n-runs "Message put!"}))))
;;               10)]
;;       (s/start-sentinel! s)
;;       (<!! (async/timeout 500))
;;       (let [qname (s/get-queue-name s)
;;             consumer (s/get-queue-group s)
;;             [msgs ?err] (q/read-next! 1 :from qname :as consumer :block 500)]
;;         (is (nil? ?err))
;;         (is (= (count msgs) 1))
;;         (is (= (-> msgs first :msg) {"1" "Message put!"}))))))
