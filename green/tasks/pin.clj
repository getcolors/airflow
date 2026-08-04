(ns pin (:require [clojure.java.io :as io] [clojure.java.shell :as sh] [clojure.string :as str]))
(def sites
  [{:path "../skills/package-airflow-green/green" :rx #"\(def \^:private airflow-sha (nil|\"[0-9a-f]{40}\")\)" :render pr-str}
   {:path "../skills/package-airflow-red/red" :rx #"github:getcolors/airflow#([0-9a-f]{40})" :render identity}
   {:path "../skills/package-airflow-blue/blue" :rx #"getcolors/airflow.git\", rev = \"([0-9a-f]{40})\"" :render identity}])
(defn git-out [& args] (let [{:keys [exit out]} (apply sh/sh "git" "-C" "." args)] (when (zero? exit)(str/trim out))))
(defn replace-pin [text {:keys [rx render]} sha] (let [m(re-matcher rx text)](when(.find m)(str(subs text 0(.start m 1))(render sha)(subs text(.end m 1))))))
(defn pin []
 (let [dirty(git-out "status" "--porcelain") sha(git-out "rev-parse" "HEAD") remotes(git-out "branch" "-r" "--contains" (str sha))]
  (cond (seq dirty){:green/exit 2 :green/err "airflow working tree is dirty; commit before pinning"}
        (not(str/includes?(str remotes)"origin/")){:green/exit 2 :green/err(str "airflow HEAD "(subs sha 0 7)" is not on a remote branch; push before pinning")}
        :else(let [errors(atom []) changed(atom 0)]
          (doseq [site sites](let [f(io/file(:path site)) text(when(.exists f)(slurp f)) next(when text(replace-pin text site sha))]
            (if next(do(when(not= text next)(spit f next)(swap! changed inc)))(swap! errors conj(str "could not locate pin in "(:path site))))))
          (if(seq @errors){:green/exit 2 :green/err(str/join "\n" @errors)}{:green/exit 0 :green/err(str "pinned " @changed " launcher(s) to "(subs sha 0 7))})))))
(let [{:green/keys[exit err]}(pin)](when err(binding[*out*(if(zero? exit)*out* *err*)](println err)))(System/exit exit))
