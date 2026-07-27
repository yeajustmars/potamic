(ns potamic.validation-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.validation :as v]
            [potamic.test-util :as tu]))

(deftest test-f
  (testing "potamic.validation/f"
    (let [[k err-map f] (v/f int? "Invalid integer")]
      (is (= :fn k))
      (is (= {:error/message "Invalid integer"} err-map))
      (is (fn? f)))))

(deftest test-invalidate
  (testing "potamic.validation/invalidate"
    (is (nil? (v/invalidate int? 1)))
    (is (nil? (v/invalidate [:enum 1 2 3] 2)))
    (is (nil? (v/invalidate [:map {:closed true} [:a int?] [:b string?]]
                            {:a 52 :b "ok"})))
    (is (= ["invalid type"]
           (v/invalidate [:map {:closed true} [:a int?] [:b string?]]
                         [:my :vector])))
    (is (= {:a ["missing required key"]
            :b ["missing required key"]
            :c ["disallowed key"]
            :d ["disallowed key"]}
           (v/invalidate [:map {:closed true} [:a int?] [:b string?]]
                         {:c 1 :d "ok"})))))

(deftest test-valid-redis-uri?
  (testing "potamic.validation/valid-redis-uri?"
    (doseq [uri tu/valid-redis-uris]
      (is (v/valid-redis-uri? uri)))))
