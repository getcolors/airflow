(ns io.github.getcolors.airflow.github-test
  "The DAG repository, its deploy key, and the ordering that makes a first run
  work."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [io.github.getcolors.airflow.github :as github]
   [io.github.getcolors.airflow.validate-test :refer [fixture]]))

(defn- with-keys
  [& {:as overrides}]
  (merge (fixture)
         {:airflow/deploy-keys [{:github "fixture/airflow-dags"
                                 :public "ssh-ed25519 AAAA comment"
                                 :private-file "/tmp/key-0"}]
          :airflow/known-hosts "203.0.113.7 ssh-ed25519 AAAA"
          :airflow/seed-files [{:path "dags/hello.py" :content "print('hi')\n"}]
          :ip "203.0.113.7"}
         overrides))

(defn- argv-of
  [commands]
  (mapv #(vec (:args %)) commands))

(defn- flat
  [commands]
  (str/join " " (flatten (argv-of commands))))

;; ---------------------------------------------------------------------------
;; keys

(deftest the-key-comment-carries-no-timestamp
  (testing "it is rendered into an artifact golden.sh diffs, so a clock reading
            would break every run against the previous one — and the reconciler
            on the box groups generations by string equality on this value"
    (is (= "airflow-deploy-airflow-fixture-fixture-airflow-dags"
           (github/key-comment (fixture))))
    (is (= (github/key-comment (fixture)) (github/key-comment (fixture)))))
  (testing "the slash is slugged because this sits in an authorized_keys comment"
    (is (not (str/includes? (github/key-comment (fixture)) "/")))))

(deftest the-build-placeholder-never-changes
  (testing "generation is a create-time side effect, so a build needs a fixed
            value or every golden would fail against the last one"
    (let [a (github/placeholder-keys (fixture))
          b (github/placeholder-keys (fixture))]
      (is (= a b))
      (is (= 1 (count a)))
      (is (str/includes? (:public (first a)) "BUILDPLACEHOLDER"))
      (is (str/ends-with? (:public (first a)) (github/key-comment (fixture))))))
  (testing "and no repository means no key at all"
    (is (= [] (github/placeholder-keys (dissoc (fixture) :dags-repo))))))

(deftest a-known-hosts-line-drops-the-servers-own-comment
  (testing "the comment is the server's hostname at key generation time and
            means nothing to a client"
    (is (= "203.0.113.7 ssh-ed25519 AAAAC3Nz"
           (github/known-hosts-line "203.0.113.7"
                                    "ssh-ed25519 AAAAC3Nz root@airflow\n")))
    (is (nil? (github/known-hosts-line "203.0.113.7" "garbage")))
    (is (nil? (github/known-hosts-line "203.0.113.7" "")))))

;; ---------------------------------------------------------------------------
;; the commands

(deftest a-first-run-creates-then-publishes-then-seeds
  (testing "the ordering is load-bearing and is the reverse of what reads
            naturally: pushing the seed first triggers the workflow before the
            key it needs exists, which looks like a broken install"
    (let [cmds (github/create-commands (with-keys) {:exists? false
                                                    :seed-files [{:path "dags/hello.py"
                                                                  :content "x"}]})
          labels (mapv :label cmds)
          index (fn [fragment]
                  (first (keep-indexed #(when (str/includes? %2 fragment) %1) labels)))]
      (is (< (index "create ") (index "SSH_PRIVATE_KEY")))
      (is (< (index "SSH_PRIVATE_KEY") (index "seed")))))
  (testing "and the repository is created private, explicitly rather than by
            inheriting the account default"
    (is (str/includes? (flat (github/create-repo-commands (fixture))) "--private"))))

(deftest a-lost-race-is-repaired-by-the-next-create
  "The failure this rule exists for, and it is not hypothetical.

  A PUT to the contents API immediately after `gh repo create` returns 404 while
  GitHub finishes initialising the repository. The original rule seeded only a
  repository the same run had just made, so when that race was lost the
  repository stayed empty forever — every later create saw it already existed
  and skipped seeding. That is exactly how the first real deployment ended up
  with a created, private and completely empty DAG repository.

  Keyed on the missing file instead, the repair happens on the next create."
  (let [cmds (github/create-commands
              (with-keys)
              {:exists? true                       ;; created by a previous run
               :seed-files [{:path "dags/hello.py" :content "x"}]})
        text (flat cmds)]
    (is (not (str/includes? text "repo create"))
        "the repository is not created twice")
    (is (str/includes? text "contents/dags/hello.py")
        "but the file that never landed is written now")))

(deftest a-file-that-exists-is-never-written
  "The property that actually matters, and it is stronger than the rule it
  replaced: a path present in the repository is not written even though the
  repository is not new, so the operator's own hello_world.py survives."
  (let [calls (atom [])
        runner (fn [args _ _]
                 (swap! calls conj (str/join " " args))
                 ;; `gh api ... contents/<path>` exit 0 means the file is there
                 {:exit 0 :out "" :err ""})]
    (is (not (github/seed-file-missing? (fixture) runner "dags/hello_world.py")))
    (is (str/includes? (first @calls) "contents/dags/hello_world.py")))
  (testing "and a 404 reports it missing, which also covers a repository that is
            not ready yet — a brand-new one reports every seed file missing,
            which is correct"
    (is (github/seed-file-missing? (fixture)
                                   (fn [_ _ _] {:exit 1 :out "" :err "Not Found"})
                                   "dags/hello_world.py"))))

(deftest the-seed-writes-are-retried
  "So a first create does not depend on winning the race at all. Retried rather
  than preceded by a fixed sleep: the delay is GitHub's and not knowable, and a
  sleep long enough to be safe is one nobody wants on every create."
  (let [cmds (github/seed-commands (fixture) [{:path "dags/hello.py" :content "x"}])
        {:keys [times delay-ms]} (:retry (first cmds))]
    (is (some? (:retry (first cmds))) "seed writes must carry a retry policy")
    (is (> times 1))
    (is (pos? delay-ms))
    (testing "and the credential writes do not — a failure there is real"
      (is (nil? (:retry (first (github/publish-commands
                                (with-keys)
                                (first (:airflow/deploy-keys (with-keys))))))))))
  (testing "the step retries a seed that 404s and then succeeds"
    (let [attempts (atom 0)
          runner (fn [args _ _]
                   (let [cmd (str/join " " args)
                         ;; Both the seed write and the ENVIRONMENT creation are
                         ;; PUTs, so matching on "PUT" alone counts the wrong
                         ;; command — which is how the first version of this
                         ;; test failed against correct code.
                         seed-write? (and (str/includes? cmd "--method PUT")
                                          (str/includes? cmd "contents/"))]
                     (cond
                       (str/includes? cmd "repo view") {:exit 1 :out "" :err "not found"}
                       seed-write?
                       (do (swap! attempts inc)
                           (if (< @attempts 3)
                             {:exit 1 :out "" :err "gh: Not Found (HTTP 404)"}
                             {:exit 0 :out "" :err ""}))
                       ;; the existence probe: report the file missing
                       (str/includes? cmd "contents/") {:exit 1 :out "" :err "Not Found"}
                       :else {:exit 0 :out "" :err ""})))
          result (github/github-step
                  (assoc (with-keys) :green/event :create) runner)]
      (is (= 0 (:green/exit result))
          "a create must survive the initialisation race")
      (is (>= @attempts 3) "and must actually have retried"))))

(deftest an-existing-repository-is-never-created-and-its-files-are-never-rewritten
  (testing "Colors converges on every create and DAGs are the user's work, so a
            converging seed would overwrite real DAGs with the example. The
            guard is now the file rather than the repository — `seed-files` is
            handed in already narrowed to the missing ones, so a repository
            whose seed is intact produces no write at all."
    (let [cmds (github/create-commands (with-keys) {:exists? true
                                                    :seed-files []})
          text (flat cmds)]
      (is (not (str/includes? text "repo create")))
      (is (not (str/includes? text "contents/"))
          "nothing is written when every seed file is already present")
      (testing "but the credentials are still reconciled"
        (is (str/includes? text "SSH_PRIVATE_KEY"))
        (is (str/includes? text "SERVER_IP"))))))

(deftest the-private-key-is-redirected-rather-than-passed
  (testing "green.process closes a child's stdin so it cannot be piped, and an
            argument would put the key in the process table"
    (let [cmd (first (filter #(str/includes? (:label %) "SSH_PRIVATE_KEY")
                             (github/publish-commands (with-keys)
                                                      (first (:airflow/deploy-keys
                                                              (with-keys))))))]
      (is (= ["sh" "-c"] (take 2 (:args cmd))))
      (is (str/includes? (last (:args cmd)) "< '/tmp/key-0'"))
      (is (not (str/includes? (last (:args cmd)) "ssh-ed25519")))))
  (testing "while the address and login are variables, not secrets — DNS already
            reveals the host and masking them only makes CI logs harder to read"
    (let [text (flat (github/publish-commands (with-keys)
                                              (first (:airflow/deploy-keys (with-keys)))))]
      (is (str/includes? text "variable set SERVER_IP"))
      (is (str/includes? text "variable set SERVER_USER"))
      (is (str/includes? text "secret set SSH_PRIVATE_KEY")))))

(deftest the-published-login-is-the-unprivileged-deploy-account
  (is (= "deploy" github/deploy-user))
  (is (str/includes? (flat (github/publish-commands
                            (with-keys)
                            (first (:airflow/deploy-keys (with-keys)))))
                     "--body deploy")))

(deftest the-environment-is-created-before-anything-is-written-into-it
  (testing "writing into an environment that does not exist is a 404"
    (let [cmds (github/publish-commands (with-keys)
                                        (first (:airflow/deploy-keys (with-keys))))]
      (is (str/includes? (:label (first cmds)) "environment"))
      (is (= ["gh" "api" "--method" "PUT" "--silent"
              "repos/fixture/airflow-dags/environments/airflow-fixture"]
             (vec (:args (first cmds))))))))

(deftest the-seed-names-the-branch-the-workflow-syncs-from
  (testing "an empty repository takes its default branch from the first file
            written to it, so naming it here is what makes the two agree"
    (let [text (flat (github/seed-commands (fixture) [{:path "dags/hello.py"
                                                       :content "x"}]))]
      (is (str/includes? text "branch=main"))
      (is (str/includes? text "repos/fixture/airflow-dags/contents/dags/hello.py"))
      (testing "and the content is base64, as the contents API requires"
        (is (str/includes? text "content=eA=="))))))

(deftest delete-withdraws-the-credentials-and-keeps-the-repository
  (testing "destroying compute is recoverable through WAL-G; destroying the
            repository is not, and it is the one artifact here that cannot be
            rebuilt from desired state"
    (let [text (flat (github/revoke-commands (fixture)))]
      (is (str/includes? text "secret delete SSH_PRIVATE_KEY"))
      (is (str/includes? text "variable delete SERVER_IP"))
      (is (not (str/includes? text "repo delete")))
      (is (not (str/includes? text "repo create"))))))

;; ---------------------------------------------------------------------------
;; the step

(defn- recording-runner
  "A runner that records argv and answers `responses` by matching a fragment."
  [calls responses]
  (fn [args _ _]
    (swap! calls conj (vec args))
    (or (some (fn [[fragment response]]
                (when (str/includes? (str/join " " args) fragment) response))
              responses)
        {:exit 0 :out "" :err ""})))

(deftest a-build-reaches-github-not-at-all
  (testing "wire-fn runs the same branch for build and create, so the event
            check here is what keeps a build off the network — side-effecting
            steps only cover --dry-run"
    (let [calls (atom [])
          result (github/github-step (assoc (with-keys) :green/event :build)
                                     (recording-runner calls {}))]
      (is (= [] @calls))
      (is (nil? (:green/exit result))))))

(deftest a-create-publishes-and-then-forgets-the-private-key
  (let [calls (atom [])
        result (github/github-step
                (assoc (with-keys) :green/event :create :airflow/key-dir nil)
                (recording-runner calls {"ssh_host_ed25519" {:exit 0
                                                             :out "ssh-ed25519 AAAA host"
                                                             :err ""}
                                         "repo view" {:exit 1 :out "" :err "not found"}}))]
    (is (= 0 (:green/exit result)))
    (testing "the repository was missing, so it was created and seeded"
      (is (some #(str/includes? (str/join " " %) "repo create") @calls))
      (is (some #(str/includes? (str/join " " %) "contents/") @calls)))
    (testing "and no private key survives in opts"
      (is (every? #(not (contains? % :private-file)) (:airflow/deploy-keys result))))))

(deftest a-failed-revoke-is-not-an-error
  (testing "delete has to be re-runnable, and a missing secret is the state it
            is trying to reach"
    (let [result (github/github-step
                  (assoc (with-keys) :green/event :delete)
                  (fn [_ _ _] {:exit 1 :out "" :err "not found"}))]
      (is (= 0 (:green/exit result))))))

(deftest a-failed-publish-is-an-error
  (let [result (github/github-step
                (assoc (with-keys) :green/event :create)
                (fn [args _ _]
                  (if (str/includes? (str/join " " args) "variable set")
                    {:exit 1 :out "" :err "no such environment"}
                    {:exit 0 :out "" :err ""})))]
    (is (= 1 (:green/exit result)))
    (is (str/includes? (:green/err result) "gh failed for"))))

(deftest the-host-key-is-read-over-the-administrative-connection
  (testing "one round trip, and it gets the key type without guessing — rather
            than scanning for it"
    (let [args (github/host-key-args (with-keys))]
      (is (= "ssh" (first args)))
      (is (str/includes? (str/join " " args) "root@203.0.113.7"))
      (is (str/includes? (str/join " " args) "ssh_host_ed25519_key.pub")))))
