(ns io.github.getcolors.airflow.workflow-test
  "The validation gates, the graph shape, the backends, and one whole build."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [io.github.getcolors.airflow.tools :as tools]
   [io.github.getcolors.airflow.validate-test :refer [fixture]]
   [io.github.getcolors.airflow.workflow :as workflow]))

(defn- temp-dir
  []
  (str (java.nio.file.Files/createTempDirectory
        "airflow-wf" (into-array java.nio.file.attribute.FileAttribute []))))

(def ^:private every-credential
  "Enough COLORS_PAR_* to satisfy a real create, so a test of one gate is not
  quietly passing on a different one."
  {"COLORS_PAR_DO_TOKEN" "t"
   "COLORS_PAR_CLOUDFLARE_API_TOKEN" "t"
   "COLORS_PAR_RESEND_API_KEY" "t"
   "COLORS_PAR_RESEND_PASSWORD" "t"
   "COLORS_PAR_GITHUB_TOKEN" "t"
   "COLORS_PAR_POSTGRES_PASSWORD" "t"
   "COLORS_PAR_AIRFLOW_FERNET_KEY" "t"
   "COLORS_PAR_AIRFLOW_ADMIN_PASSWORD" "t"
   "COLORS_PAR_WALG_R2_ACCESS_KEY_ID" "t"
   "COLORS_PAR_WALG_R2_SECRET_ACCESS_KEY" "t"})

(defn- start
  [event env & {:as overrides}]
  (workflow/start-step (merge (fixture) {:green/event event} overrides) env))

;; ---------------------------------------------------------------------------
;; the gates

(deftest a-build-needs-no-credential-at-all
  (testing "it renders from desired state alone and reaches no provider, so it
            has to stay usable on a fresh checkout"
    (is (= 0 (:green/exit (start :build {}))))))

(deftest a-dry-run-needs-none-either
  (is (= 0 (:green/exit (start :create {} :green/dry-run true)))))

(deftest a-real-create-demands-them
  (let [result (start :create {})]
    (is (= 2 (:green/exit result)))
    (is (str/includes? (:green/err result) "COLORS_PAR_POSTGRES_PASSWORD")))
  (is (= 0 (:green/exit (start :create every-credential)))))

(deftest an-invalid-desired-state-stops-before-anything-runs
  (let [result (start :build {} :airflow-host "not a host")]
    (is (= 2 (:green/exit result)))
    (is (str/includes? (:green/err result) ":airflow-host"))))

(deftest the-profile-parameter-is-refused-even-on-a-build
  (testing "the overlay happens before any step runs, so refusing is the only
            available answer — and for three of the four stages the profile is
            the ONLY thing separating this project's state from once-colors'"
    (let [result (start :build {"COLORS_PAR_PROFILE" "once-colors"})]
      (is (= 2 (:green/exit result)))
      (is (str/includes? (:green/err result) "COLORS_PAR_PROFILE"))
      (testing "and the overlaid value IS still sitting in opts, which is the
                whole point — read-pars overwrote the file's profile before any
                step could look at it, so there was never a correct value left
                to compare against. Refusing to run is the only answer available,
                and a non-zero exit is what makes it stick."
        (is (= "once-colors" (:profile result)))))))

(deftest a-delete-is-protected-until-it-is-unprotected
  (testing "the metadata database lives on the boot volume and a destroy takes
            it with the droplet"
    (let [result (start :delete every-credential)]
      (is (= 2 (:green/exit result)))
      (is (str/includes? (:green/err result) "COLORS_PAR_COMPUTE_PREVENT_DESTROY"))))
  (is (= 0 (:green/exit (start :delete
                               (assoc every-credential
                                      "COLORS_PAR_COMPUTE_PREVENT_DESTROY" "false"))))))

(deftest a-build-takes-a-fixed-placeholder-key
  (testing "a fresh key on every build would break every golden against the last"
    (let [result (start :build {})]
      (is (= 1 (count (:airflow/deploy-keys result))))
      (is (str/includes? (:public (first (:airflow/deploy-keys result)))
                         "BUILDPLACEHOLDER"))
      (is (nil? (:airflow/key-dir result))))))

(deftest defaults-fill-in-what-desired-state-omits
  (let [result (workflow/start-step {:green/event :build
                                     :profile "d"
                                     :workdir "/tmp/x"} {})]
    (is (= "digitalocean" (:provider-compute result)))
    (is (= "cloudflare" (:provider-dns result)))
    (is (= "resend" (:provider-smtp result)))
    (is (= "local" (:provider-backend result)))
    (is (true? (:compute-prevent-destroy result))
        "protection defaults on, not off")))

;; ---------------------------------------------------------------------------
;; the graph

(defn- graph
  "step -> next steps, for one event."
  [event]
  (let [run-opts {:green/event event}]
    (into {}
          (map (fn [step] [step (vec (rest (workflow/wire-fn step run-opts)))]))
          (case event
            :delete [:airflow/start :airflow/github :airflow/ansible-cleanup
                     :airflow/smtp-post :airflow/dns :airflow/smtp :airflow/compute]
            [:airflow/start :airflow/compute :airflow/smtp :airflow/dns
             :airflow/smtp-post :airflow/ansible-local :airflow/ansible-remote
             :airflow/github]))))

(deftest create-runs-smtp-dns-smtp-post-in-series
  (testing "the Resend sending domain must exist before its verification records
            can be rendered into DNS, and DNS must be live before verification
            runs — so these three cannot be collapsed"
    (let [g (graph :create)]
      (is (= [:airflow/compute] (:airflow/start g)))
      (is (= [:airflow/smtp] (:airflow/compute g))
          "compute and smtp run in series where ONCE forks them: one less
           concurrency edge, and joined-params reads compute off opts instead")
      (is (= [:airflow/dns] (:airflow/smtp g)))
      (is (= [:airflow/smtp-post] (:airflow/dns g))))))

(deftest github-follows-ansible-remote-rather-than-forking-beside-it
  "The seed push triggers the deploy workflow, and that workflow rsyncs to the
  server with the key this create just issued. Run in parallel with
  `ansible-remote`, the matching public key is not on the box yet and CI fails
  with `Permission denied (publickey)` on every create that seeds.

  That is not hypothetical — it is what the first successful seed did. The plan
  said `github` runs after `ansible-remote` in prose and drew a three-way fork
  in its diagram; the fork is what got built."
  (let [g (graph :create)]
    (is (= [:airflow/ansible-local :airflow/ansible-remote]
           (:airflow/smtp-post g))
        "smtp-post forks into the two Ansible stages only")
    (is (= [:airflow/github] (:airflow/ansible-remote g))
        "and github waits for the machine to have the key")
    (is (= [] (:airflow/ansible-local g)))
    (is (= [] (:airflow/github g)))))

(deftest delete-revokes-first-and-destroys-last
  (testing "a withdrawn credential against a live host is a loud, recoverable
            broken deploy; a live credential against a destroyed host is silent"
    (let [g (graph :delete)]
      (is (= [:airflow/github] (:airflow/start g)))
      (is (= [:airflow/ansible-cleanup] (:airflow/github g)))
      (is (= [:airflow/smtp-post] (:airflow/ansible-cleanup g)))
      (is (= [:airflow/dns] (:airflow/smtp-post g)))
      (is (= [:airflow/smtp :airflow/compute] (:airflow/dns g)))
      (is (= [] (:airflow/compute g))))))

(deftest build-and-create-run-the-same-graph
  (is (= (graph :create) (graph :build))))

(deftest every-step-that-reaches-a-provider-is-skipped-by-dry-run
  (let [covered (set workflow/side-effecting-steps)]
    (doseq [step [:airflow/compute :airflow/smtp :airflow/dns :airflow/smtp-post
                  :airflow/ansible-local :airflow/ansible-remote
                  :airflow/ansible-cleanup :airflow/github]]
      (is (contains? covered step) (str step " must be skipped by --dry-run")))
    (is (not (contains? covered :airflow/start))
        "start validates; it must always run")))

;; ---------------------------------------------------------------------------
;; backends

(deftest the-compute-stage-is-keyed-by-this-packages-own-name
  (testing "a colliding profile still cannot produce ONCE's compute state key"
    (let [opts (assoc (fixture) :provider-backend "r2" :workdir (temp-dir))
          advice (workflow/own-backend-advice tools/compute-tool)]
      (advice opts)
      (is (str/includes?
           (slurp (str (tools/tool-dir opts tools/compute-tool) "/backend.tf.json"))
           "airflow-fixture/airflow-compute.tfstate")))))

(deftest the-delegated-stages-are-keyed-by-ONCEs-names
  (testing "not a choice this package gets to make: each ONCE step computes its
            own directory internally, so renaming would mean forking all three"
    (doseq [tool [tools/dns-tool tools/smtp-tool tools/smtp-post-tool]]
      (let [opts (assoc (fixture) :provider-backend "r2" :workdir (temp-dir))
            advice (workflow/delegated-backend-advice tool)]
        (advice opts)
        (is (str/includes?
             (slurp (str (tools/delegated-tool-dir opts tool) "/backend.tf.json"))
             (str "airflow-fixture/" tool ".tfstate")))))))

(deftest the-backend-advice-writes-where-the-delegated-step-will-run
  (testing "two copies of that resolution agreeing today is not the same as
            agreeing after a pin bump, which is why one of them is ONCE's"
    (let [opts (assoc (fixture) :provider-backend "r2" :workdir (temp-dir))]
      ((workflow/delegated-backend-advice tools/dns-tool) opts)
      (is (.exists (io/file (tools/delegated-tool-dir opts tools/dns-tool)
                            "backend.tf.json"))))))

(deftest a-local-backend-writes-no-remote-key
  (let [opts (assoc (fixture) :provider-backend "local" :workdir (temp-dir))]
    ((workflow/own-backend-advice tools/compute-tool) opts)
    (let [json (slurp (str (tools/tool-dir opts tools/compute-tool)
                           "/backend.tf.json"))]
      (is (str/includes? json "local"))
      (is (not (str/includes? json "airflow-fixture/airflow-compute.tfstate"))))))

;; ---------------------------------------------------------------------------
;; a whole build

(deftest a-build-renders-every-stage-this-package-owns
  (testing "the delegated three are ONCE's and are covered by golden.sh, which
            renders them through the real steps rather than around them"
    (let [opts (assoc (fixture) :green/event :build :workdir (temp-dir))]
      (tools/compute-step opts)
      (tools/ansible-local-step opts)
      (tools/ansible-remote-step opts)
      (tools/seed-step opts)
      (doseq [tool [tools/compute-tool tools/ansible-local-tool
                    tools/ansible-remote-tool tools/github-tool]]
        (is (.isDirectory (io/file (tools/tool-dir opts tool)))
            (str tool " must render"))))))
