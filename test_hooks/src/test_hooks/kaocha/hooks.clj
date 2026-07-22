(ns test-hooks.kaocha.hooks)

(defn clear-screen
  [test-plan]
  (print "\033[H\033[2J\033[3J")
  (flush)
  test-plan)
