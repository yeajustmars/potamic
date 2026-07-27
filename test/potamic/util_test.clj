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
