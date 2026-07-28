(ns potamic.fmt-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as string]
            [potamic.fmt :as fmt]))

(deftest test__LOGO
  (testing "potamic.fmt/LOGO"
    (is (=
"
 ___     _              _
| _ \\___| |_ __ _ _ __ (_)__
|  _/ _ \\  _/ _` | '  \\| / _|
|_| \\___/\\__\\__,_|_|_|_|_\\__|
"
fmt/LOGO
)))) ;; WARN: Do not auto-format this and add indentation!

(deftest test__BLUE   (testing "potamic.fmt/BLUE"   (is (= "\033[0;34m" fmt/BLUE  ))))
(deftest test__BOLD   (testing "potamic.fmt/BOLD"   (is (= "\033[1m"    fmt/BOLD  ))))
(deftest test__CYAN   (testing "potamic.fmt/CYAN"   (is (= "\033[0;36m" fmt/CYAN  ))))
(deftest test__GREEN  (testing "potamic.fmt/GREEN"  (is (= "\033[0;32m" fmt/GREEN ))))
(deftest test__ITAL   (testing "potamic.fmt/ITAL"   (is (= "\033[3m"    fmt/ITAL  ))))
(deftest test__NC     (testing "potamic.fmt/NC"     (is (= "\033[0m"    fmt/NC    ))))
(deftest test__ORANGE (testing "potamic.fmt/ORANGE" (is (= "\033[0;33m" fmt/ORANGE))))
(deftest test__PURPLE (testing "potamic.fmt/PURPLE" (is (= "\033[0;35m" fmt/PURPLE))))
(deftest test__RED    (testing "potamic.fmt/RED"    (is (= "\033[0;31m" fmt/RED   ))))

(deftest test__echo
  (testing "potamic.fmt/echo"
    (is (= (str fmt/PURPLE "[DEBUG]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :debug "abc"))))

    (is (= (str fmt/RED "[ERROR]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :error "abc"))))

    (is (= (str fmt/GREEN "[HELP]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :help "abc"))))

    (is (= (str fmt/CYAN "[HINT]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :hint "abc"))))

    (is (= (str fmt/BLUE "[INFO]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :info "abc"))))

    (is (= (str fmt/GREEN "[SUCCESS]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :success "abc"))))

    (is (= (str fmt/RED "[WARN]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :warn "abc"))))

    (is (= (str fmt/RED "[WARN]" fmt/NC " abc\n")
           (with-out-str (fmt/echo :warn "abc"))))))

(deftest test__make-prefix
  (testing "potamic.fmt/make-prefix"
    (is (= (str fmt/PURPLE "[DEBUG]"   fmt/NC) (fmt/make-prefix :debug  )))
    (is (= (str fmt/RED    "[ERROR]"   fmt/NC) (fmt/make-prefix :error  )))
    (is (= (str fmt/GREEN  "[HELP]"    fmt/NC) (fmt/make-prefix :help   )))
    (is (= (str fmt/CYAN   "[HINT]"    fmt/NC) (fmt/make-prefix :hint   )))
    (is (= (str fmt/BLUE   "[INFO]"    fmt/NC) (fmt/make-prefix :info   )))
    (is (= (str fmt/GREEN  "[SUCCESS]" fmt/NC) (fmt/make-prefix :success)))
    (is (= (str fmt/RED    "[WARN]"    fmt/NC) (fmt/make-prefix :warn   )))))

(deftest test__pretty-clj
  (testing "potamic.fmt/pretty-clj"
    (is (= "{:a 1, :b 2}\n" (fmt/pretty-clj {:a 1 :b 2})))
    (is (= "[1 2 3]\n" (fmt/pretty-clj [1 2 3])))))
