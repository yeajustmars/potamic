(ns potamic.util-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.test-util :as tu]
            [potamic.util :as util]))

(deftest test__->str
  (testing "potamic.util/->str"
    (let [should-pass {:my/queue "my/queue"
                       'my/queue "my/queue"
                       "my/queue" "my/queue"
                       :x "x"
                       'x "x"
                       "x" "x"
                       :a.b/c.d "a.b/c.d"
                       'a.b/c.d "a.b/c.d"
                       "a.b/c.d" "a.b/c.d"}]
      (doseq [[x check] should-pass]
        (is (= check (util/->str x)))))))

(deftest test__->int
  (testing "potamic.util/->int"
    (is (= 1 (util/->int "1")))
    (is (= 66 (util/->int "66")))))

(deftest test__<-str
  (testing "potamic.util/<-str"
    (is (= :x/y (util/<-str "x/y")))
    (is (= "111" (util/<-str "111")))
    (is (= 111 (util/<-str 111)))))

(deftest test__prep-cmd
  (testing "potamic.util/prep-cmd"
    (is (= ["a" "b" "c"]             (util/prep-cmd [[:a] ['b] ["c"]])))
    (is (= ["a" "b" "c"]             (util/prep-cmd [["a"] ['b] ["c"]])))
    (is (= ["a" "b" "c" "d" "e" "f"] (util/prep-cmd [[[['a]] 'b [[:c]] 'd] "e" "f"])))))

(deftest test__time->milliseconds
  (testing "potamic.util/time->milliseconds"
    (is (= 2       (util/time->milliseconds [2 :milli]   )))
    (is (= 2       (util/time->milliseconds [2 :millis]  )))
    (is (= 2000    (util/time->milliseconds [2 :second]  )))
    (is (= 2000    (util/time->milliseconds [2 :seconds] )))
    (is (= 120000  (util/time->milliseconds [2 :minute]  )))
    (is (= 120000  (util/time->milliseconds [2 :minutes] )))
    (is (= 7200000 (util/time->milliseconds [2 :hour]    )))
    (is (= 7200000 (util/time->milliseconds [2 :hours]   )))))

(deftest test__remove-conn
  (testing "potamic.util/remove-conn"
    (is (= {} (util/remove-conn {:conn {}})))
    (is (= {:a 1 :b 2 :c 3} (util/remove-conn {:conn {} :a 1 :b 2 :c 3})))))

(deftest test__parse-redis-uri
  (testing "potamic.util/parse-redis-uri"
    (doseq [[in out] tu/valid-uri-parse-mappings]
      (is (= out (util/parse-redis-uri in))))))

(deftest test__parse-redis-uri-edges
  (testing "1. Standard Redis URI with no path or DB"
    (is (= {:scheme "redis" :user nil :password nil :host "localhost" :port 6379 :db 0}
           (util/parse-redis-uri "redis://localhost:6379"))))

  (testing "2. Kvrocks URI with redis-mode query parameter"
    (is (= {:scheme "kvrocks" :user nil :password nil :host "127.0.0.1" :port 6666 :db 0 :query {"mode" "redis"}}
           (util/parse-redis-uri "kvrocks://127.0.0.1:6666?mode=redis"))))

  (testing "3. Multiple query parameters"
    (is (= {"mode" "redis" "timeout" "5000"}
           (:query (util/parse-redis-uri "kvrocks://127.0.0.1:6666/3?mode=redis&timeout=5000")))))

  (testing "4. URI with only password (no user)"
    (is (= {:scheme "redis" :user nil :password "secret" :host "redis.internal" :port 6379 :db 2}
           (util/parse-redis-uri "redis://:secret@redis.internal:6379/2"))))

  (testing "5. URI with user but blank password"
    (is (= {:scheme "redis" :user "admin" :password nil :host "localhost" :port 6379 :db 0}
           (util/parse-redis-uri "redis://admin:@localhost:6379/0"))))

  (testing "6. Missing port defaults to nil"
    (is (= {:scheme "redis" :user nil :password nil :host "localhost" :port nil :db 1}
           (util/parse-redis-uri "redis://localhost/1"))))

  (testing "7. Invalid database path defaults to 0"
    (is (= 0 (:db (util/parse-redis-uri "redis://localhost:6379/invalidDB")))))

  (testing "8. Two-digit database index"
    (is (= 15 (:db (util/parse-redis-uri "redis://localhost:6379/15")))))

  (testing "9. Empty query parameter values"
    (is (= {"mode" "redis" "flag" ""}
           (:query (util/parse-redis-uri "kvrocks://localhost:6666/0?mode=redis&flag=")))))

  (testing "10. Strange characters in password"
    (is (= "pAs5w0rD_"
           (:password (util/parse-redis-uri "redis://user:pAs5w0rD_@localhost:6379/0"))))))
