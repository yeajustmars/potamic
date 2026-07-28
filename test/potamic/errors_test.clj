(ns potamic.errors-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.errors :as e]
            [potamic.errors.types :as et]))

(deftest test__error-types
  (testing "potamic.errors/error-types"
    (is (= #{:potamic/args-err
             :potamic/db-err
             :potamic/internal-err}
           et/error-types))))

(deftest test__error
  (testing "potamic.errors/error"
    (let [{:keys [potamic/err-file
                  potamic/err-line
                  potamic/err-column
                  potamic/err-msg
                  potamic/err-type
                  potamic/err-data]
           } (e/error {:potamic/err-type :potamic/args-err
                       :potamic/err-fatal? false
                       :potamic/err-msg "ERROR MSG"
                       :potamic/err-data {:a 1 :b 2}})]
      (is (int? err-line))
      (is (int? err-column))
      (is (not= err-line 0))
      (is (not= err-column 0))
      (is (= "potamic/errors_test.clj" err-file))
      (is (= {:a 1 :b 2} err-data))
      (is (= "ERROR MSG" err-msg))
      (is (= :potamic/args-err err-type)))))
