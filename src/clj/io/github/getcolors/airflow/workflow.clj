(ns io.github.getcolors.airflow.workflow
  "The DAG the launcher runs, and the steps that are not a tool.

      create / build   start ─ compute ─ smtp ─ dns ─ smtp-post ─┬─ ansible-local
                                                                 ├─ ansible-remote
                                                                 └─ github

      delete           start ─ github ─ ansible-cleanup ─ smtp-post ─ dns ─┬─ smtp
                                                                          └─ compute

  This is ONCE's shape rather than walter's, and the SMTP ordering is why. The
  Resend sending domain must exist before its verification records can be
  rendered into DNS, and DNS must be live before verification runs — so
  `smtp → dns → smtp-post` cannot be collapsed into fewer stages.

  Compute and SMTP run in **series** where ONCE forks them. ONCE's
  `joined-params` reads the compute output out of `:green/branches` at the DNS
  join; running in series means it reads it off `opts` instead, which is the
  same function's second fallback. One less concurrency edge for no lost time —
  the SMTP stage is an API call, not a machine build.

  `github` runs last on create, after `ansible-remote`, for ONCE's reason: the
  credentials it publishes describe a configured host, and a workstation-side
  failure should not gate them. On delete it runs **first**, revoking before
  anything is destroyed — a withdrawn credential against a live host is a loud,
  recoverable broken deploy, while a live credential against a destroyed host is
  silent.

  Delete never touches the DAG repository. Destroying compute is recoverable
  through WAL-G; destroying the repository is not, and it is the one artifact
  here that cannot be rebuilt from desired state."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [green.cli :as green-cli]
   [green.dry-run :as dry-run]
   [green.progress :as progress]
   [green.tofu :as tofu]
   [green.workflow :as wf]
   [io.github.getcolors.airflow.github :as github]
   [io.github.getcolors.airflow.tools :as tools]
   [io.github.getcolors.airflow.validate :as validate]))

(def ^:private defaults
  {:compute-prevent-destroy true
   :provider-compute "digitalocean"
   :provider-dns "cloudflare"
   :provider-smtp "resend"
   :provider-backend "local"
   :workdir ".colors"})

;; ---------------------------------------------------------------------------
;; start

(defn- state-output
  "Read a previously applied stage's `params` output, or nil when the stage has
  no state yet."
  [opts dir]
  (try
    (some-> (tofu/outputs dir (tools/backend-credential-env opts))
            :params walk/keywordize-keys)
    (catch Exception _ nil)))

(defn adopt-existing-state
  "Delete renders the same templates as create, so a destroy needs the params
  the earlier stages produced — the machine's address for the DNS records, the
  Resend domain ids for the verification ones.

  The compute output is republished under `:once/compute-params` as well as
  merged flat, because that is the key ONCE's delegated DNS step reads. Without
  it a delete would render its records against the fallback address and then
  propose destroying records that do not match what is there."
  [opts]
  (let [compute (state-output opts (tools/tool-dir opts tools/compute-tool))
        smtp (state-output opts (tools/delegated-tool-dir opts tools/smtp-tool))]
    (cond-> opts
      compute (-> (merge compute) (assoc :once/compute-params compute))
      smtp (-> (merge smtp) (assoc :once/smtp-params smtp)))))

(defn- with-deploy-keys
  "Attach the key `ansible-remote` installs and the `github` step publishes.

  Generating it is a create-time side effect, so a build or a dry-run takes a
  fixed placeholder instead: a fresh key rendered into the artifact would make
  every `golden.sh` run fail against the previous one."
  [opts real?]
  (if (and real? (= :create (:green/event opts)))
    (let [[keys err] (github/generate-keys opts)]
      (if err
        (assoc opts :green/exit 1 :green/err err)
        (assoc opts
               :green/exit 0
               :airflow/deploy-keys keys
               :airflow/key-dir (some-> (first keys) :private-file
                                        io/file .getParent))))
    (assoc opts :green/exit 0 :airflow/deploy-keys (github/placeholder-keys opts))))

(defn start-step
  "Overlay `COLORS_PAR_*`, validate, and — for a real delete — read back what
  the earlier stages left in OpenTofu state.

  Credentials are only required for an event that actually reaches a provider:
  `build` and `--dry-run` render from desired state alone, so they stay usable
  with nothing set.

  The two-argument arity takes the environment to overlay, so a test does not
  inherit whatever `COLORS_PAR_*` variables the developer happens to have set."
  ([opts] (start-step opts (System/getenv)))
  ([opts env]
   (let [opts (green-cli/read-pars (merge defaults opts) env)
         event (:green/event opts)
         real? (not (:green/dry-run opts))
         lifecycle? (contains? #{:create :delete} event)
         errors (vec (concat
                      (validate/env-errors env)
                      (validate/state-errors opts)
                      (when (and real? lifecycle?) (validate/secret-errors opts))
                      (when (and real? (= :delete event)
                                 (:compute-prevent-destroy opts))
                        [(str "compute destruction is protected; set "
                              (green-cli/par-name :compute-prevent-destroy)
                              "=false to delete")])))]
     (cond
       (seq errors) (assoc opts :green/exit 2 :green/err (str/join "\n" errors))
       (and real? (= :delete event)) (assoc (adopt-existing-state opts) :green/exit 0)
       :else (with-deploy-keys opts real?)))))

;; ---------------------------------------------------------------------------
;; the composed steps

(defn github-step
  "Render the seed, then publish.

  Two functions rather than one because `github.clj` renders nothing — it has no
  template specs and requiring `tools` from it would invert the dependency that
  lets `tools` build the authorized_keys line. Composing them here is what walter
  does with `ansible-cleanup-step`, and it keeps the seed an artifact `build`
  produces and `golden.sh` can hold still."
  [opts]
  (let [rendered (tools/seed-step opts)]
    (if (wf/failed? rendered)
      rendered
      (github/github-step rendered))))

(defn ansible-cleanup-step
  "Drop the managed `~/.ssh/config` block, then remove both rendered trees.

  ansible-local replays its playbook with block_state absent; both steps then
  scaffold against `:green/event :delete`, which deletes their targets. The alias
  comes from `profile` rather than from OpenTofu state, so this works when the
  machine is already gone.

  There is nothing to undo on the far side. A delete destroys the droplet, so
  reconciling anything on it first would be work against a machine about to stop
  existing."
  [opts]
  (-> opts tools/ansible-local-step tools/ansible-remote-step))

;; ---------------------------------------------------------------------------
;; wiring

(defn wire-fn
  [step run-opts]
  (if (= :delete (:green/event run-opts))
    (case step
      :airflow/start           [start-step :airflow/github]
      :airflow/github          [github-step :airflow/ansible-cleanup]
      :airflow/ansible-cleanup [ansible-cleanup-step :airflow/smtp-post]
      :airflow/smtp-post       [tools/smtp-post-step :airflow/dns]
      :airflow/dns             [tools/dns-step :airflow/smtp :airflow/compute]
      :airflow/smtp            [tools/smtp-step]
      :airflow/compute         [tools/compute-step])
    ;; :create and :build
    (case step
      :airflow/start          [start-step :airflow/compute]
      :airflow/compute        [tools/compute-step :airflow/smtp]
      :airflow/smtp           [tools/smtp-step :airflow/dns]
      :airflow/dns            [tools/dns-step :airflow/smtp-post]
      :airflow/smtp-post      [tools/smtp-post-step
                               :airflow/ansible-local
                               :airflow/ansible-remote
                               :airflow/github]
      :airflow/ansible-local  [tools/ansible-local-step]
      :airflow/ansible-remote [tools/ansible-remote-step]
      :airflow/github         [github-step])))

;; ---------------------------------------------------------------------------
;; backends

(defn backend-advice
  "The `:before` advice writing backend.tf.json for one stage.

  `dir-fn` is passed in rather than derived, because the four stages do not
  agree on how their directory is computed. This package's own compute stage
  uses `tools/tool-dir`; the three delegated ones are routed through ONCE's,
  since ONCE's step functions compute their own directory internally and the
  advice has to write into exactly the one the step will then run in."
  [dir-fn tool]
  (let [state-key #(str (or (:profile %) "airflow") "/" tool ".tfstate")]
    (tofu/backends
     #(or (:provider-backend %) "local")
     {"local" (tofu/local-backend-advice dir-fn)
      "s3" (tofu/s3-backend-advice dir-fn
                                   (fn [opts]
                                     {:bucket (:s3-bucket opts)
                                      :key (state-key opts)
                                      :region (:s3-region opts)}))
      "r2" (tofu/r2-backend-advice dir-fn
                                   (fn [opts]
                                     {:bucket (:r2-bucket opts)
                                      :key (state-key opts)
                                      :endpoint (:r2-endpoint opts)}))})))

(defn own-backend-advice
  "Backend advice for a stage this package renders itself."
  [tool]
  (backend-advice #(tools/tool-dir % tool) tool))

(defn delegated-backend-advice
  "Backend advice for a stage ONCE renders.

  The state key is ONCE's stage name, not one this package chose, and that is
  not an oversight — `(tool-dir opts \"tofu-dns\")` is hard-coded inside
  `tofu-dns-step`, so renaming would mean forking the three steps and forfeiting
  the reuse that motivated delegating to them. The consequence is stated where
  it belongs, in `tools`: for these three stages `profile` alone separates this
  project from a once-colors, where walter has two independent separations."
  [tool]
  (backend-advice #(tools/delegated-tool-dir % tool) tool))

(def side-effecting-steps
  [:airflow/compute :airflow/smtp :airflow/dns :airflow/smtp-post
   :airflow/ansible-local :airflow/ansible-remote :airflow/ansible-cleanup
   :airflow/github])

(def workflow
  (-> (wf/workflow {:start :airflow/start :wire-fn wire-fn})
      (wf/advice-add :airflow/compute :before ::backend
                     (own-backend-advice tools/compute-tool))
      (wf/advice-add :airflow/smtp :before ::backend
                     (delegated-backend-advice tools/smtp-tool))
      (wf/advice-add :airflow/dns :before ::backend
                     (delegated-backend-advice tools/dns-tool))
      (wf/advice-add :airflow/smtp-post :before ::backend
                     (delegated-backend-advice tools/smtp-post-tool))
      progress/advise
      (dry-run/advise side-effecting-steps)))
