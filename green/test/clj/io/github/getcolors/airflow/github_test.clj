(ns io.github.getcolors.airflow.github-test
  "The DAG repository, its deploy key, and the ordering that makes a first run
  work."
  (:require
   [clojure.java.io :as io]
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
    (let [cmds (github/create-commands (with-keys) {:exists? false})
          labels (mapv :label cmds)
          index (fn [fragment]
                  (first (keep-indexed #(when (str/includes? %2 fragment) %1) labels)))]
      (is (< (index "create ") (index "SSH_PRIVATE_KEY"))
          "the repository exists before anything is published into it")
      (is (not-any? #(str/includes? % "seed") labels)
          "and seeding is not a gh invocation at all — it goes over SSH")))
  (testing "and the repository is created private, explicitly rather than by
            inheriting the account default"
    (is (str/includes? (flat (github/create-repo-commands (fixture))) "--private")))
  (testing "and initialised, so the clone the seed does next has a branch to
            check out — `gh repo create` without this leaves a repository with no
            commits and nothing to clone"
    (is (str/includes? (flat (github/create-repo-commands (fixture))) "--add-readme"))))

(deftest an-existing-repository-is-never-created-and-its-files-are-never-rewritten
  (testing "Colors converges on every create and DAGs are the user's work, so a
            converging seed would overwrite real DAGs with the example. The
            guard is the file, checked against the clone, so a repository whose
            seed is intact produces no commit at all."
    (let [cmds (github/create-commands (with-keys) {:exists? true})
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
      (testing "and the seed is a git push over SSH, not a contents API call"
        (is (some #(str/includes? (str/join " " %) "git clone") @calls))
        (is (some #(str/includes? (str/join " " %) "push origin") @calls))
        (is (not-any? #(str/includes? (str/join " " %) "contents/") @calls))))
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

;; ---------------------------------------------------------------------------
;; seeding, over SSH

(deftest the-seed-goes-over-ssh-not-the-api
  "The whole reason seeding is a git push rather than a REST call.

  Writing under `.github/workflows/` through the API needs the `workflow` OAuth
  scope, which GitHub gates separately because a workflow file is arbitrary code
  execution in CI with that repository's secrets. Granting it to the token this
  package already holds would widen that token across every repository the org
  can see — a large concession for one example file, and one that contradicts
  the posture everywhere else here.

  A push over SSH carries no OAuth scope at all."
  (is (= "git@github.com:fixture/airflow-dags.git" (github/ssh-remote (fixture))))
  (is (not (str/includes? (github/ssh-remote (fixture)) "https")))
  (testing "and the token is never handed to git"
    (let [envs (atom [])
          runner (fn [_ env _] (swap! envs conj env) {:exit 1 :out "" :err "boom"})]
      (github/seed-repo! (with-keys) [{:path "dags/hello.py" :content "x"}] runner)
      (is (every? #(nil? (get-in % [:extra-env "GH_TOKEN"])) @envs)
          "git authenticates with the operator's SSH key, not the GitHub token"))))

(deftest seeding-writes-only-what-is-missing-and-pushes-once
  (let [calls (atom [])
        runner (fn [args _ _]
                 (swap! calls conj (vec args))
                 {:exit 0 :out "" :err ""})
        [seeded err] (github/seed-repo!
                      (with-keys)
                      [{:path "dags/hello.py" :content "x"}
                       {:path ".github/workflows/deploy-dags.yml" :content "y"}]
                      runner)
        argv (mapv #(str/join " " %) @calls)]
    (is (nil? err))
    (is (= ["dags/hello.py" ".github/workflows/deploy-dags.yml"] seeded))
    (is (str/includes? (first argv) "git clone"))
    (is (str/includes? (first argv) "git@github.com:fixture/airflow-dags.git"))
    (is (some #(str/includes? % "commit") argv))
    (is (some #(str/includes? % "push origin HEAD:main") argv)
        "pushed to dags-branch, which is created if it does not exist")
    (testing "and the commit identity is set per invocation rather than assumed"
      (is (some #(str/includes? % "-c user.email=") argv)))))

(deftest an-intact-seed-produces-no-commit
  "A path present in the clone is never written, so the operator's own
  hello_world.py survives a create even though the repository is not new."
  (let [calls (atom [])
        runner (fn [args _ _]
                 (swap! calls conj (vec args))
                 ;; the clone populates the working tree, as a real one would
                 (when (= "clone" (second args))
                   (let [dir (last args)
                         f (io/file dir "dags/hello.py")]
                     (io/make-parents f)
                     (spit f "the operator's own DAG")))
                 {:exit 0 :out "" :err ""})
        [seeded err] (github/seed-repo!
                      (with-keys) [{:path "dags/hello.py" :content "SEED"}] runner)
        argv (mapv #(str/join " " %) @calls)]
    (is (nil? err))
    (is (= [] seeded))
    (is (not-any? #(str/includes? % "commit") argv))
    (is (not-any? #(str/includes? % "push") argv))))

(deftest a-clone-that-fails-says-it-needs-an-ssh-key
  (let [[_ err] (github/seed-repo!
                 (with-keys) [{:path "dags/hello.py" :content "x"}]
                 (fn [_ _ _] {:exit 128 :out "" :err "Permission denied (publickey)."}))]
    (is (some? err))
    (is (str/includes? err "SSH"))
    (is (str/includes? err "ssh -T git@github.com")
        "and names the command that checks it")))
