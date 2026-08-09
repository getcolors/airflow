(ns io.github.getcolors.airflow.tools
  "The step functions, their template specs, and the generated inventory.

  This package reuses far more of ONCE than walter does, and the difference is
  worth stating rather than discovering. Walter consumes two things: the
  provider registry as data, and the compute templates by classpath keyword.
  Both are *resources*, which `scripts/golden.sh` can hold still.

  This package consumes six, and three of them are step **functions**:

    1. `once.validate/providers`           — the registry, as data
    2. `once.tools.tofu.<provider>/main.tf` — the compute template, by classpath
    3. `once.tools/tofu-smtp-step`          — a function
    4. `once.tools/tofu-dns-step`           — a function
    5. `once.tools/tofu-smtp-post-step`     — a function
    6. `once.tools/tool-dir`                — a function, for the three above

  Nothing upstream promises any of that. ONCE's `utils/contract` versions the
  launcher handshake, not a library API, and ONCE's own rules treat its
  internals as free to move as long as three colours move together. The three
  step functions are the exposed part: they take `opts` and return `opts`, so a
  changed key inside one fails at run time rather than at compile time.

  Two consequences follow from delegating, and both are load-bearing:

  * **The delegated stage directories cannot be renamed.** Each ONCE step
    hard-codes its own — `(tool-dir opts \"tofu-dns\")` is inside
    `tofu-dns-step`, not a parameter. Renaming means forking the three steps,
    which forfeits the reuse that motivated delegating at all. So this package's
    state keys are `<profile>/airflow-compute.tfstate` for its own stage but
    `<profile>/tofu-dns.tfstate`, `<profile>/tofu-smtp.tfstate` and
    `<profile>/tofu-smtp-post.tfstate` for the three delegated ones — the same
    keys ONCE writes. `profile` alone is what separates this project from a
    once-colors, where walter has two independent separations.

  * **`tofu-dns-step` reads `:once/compute-params`** to find the address it
    points DNS at. That is an internal key of ONCE's, not a documented
    interface, and this package's own compute step publishes it deliberately.

  `scripts/golden.sh` is the whole mitigation. Bump the ONCE pin deliberately
  and read the diff rather than accepting it."
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [green.ansible :as ansible]
   [green.cli :as green-cli]
   [green.providers :as provider-ops]
   [green.scaffold :as sc]
   [green.tofu :as tofu]
   [green.workflow :as wf]
   [io.github.getcolors.airflow.github :as github]
   [io.github.getcolors.airflow.utils :as utils]
   [io.github.getcolors.airflow.validate :as validate]
   [io.github.getcolors.once.tools :as once-tools]))

;; ---------------------------------------------------------------------------
;; stage names

(def compute-tool
  "The compute stage directory, and half of its OpenTofu state key.

  Deliberately not `tofu-compute`. Remote state is keyed `<profile>/<tool>` and
  nothing but convention keeps this project's profile distinct from another's,
  so an airflow-specific stage name means a colliding profile still cannot
  produce ONCE's compute state key.

  The three delegated stages get no such protection — see the namespace
  docstring."
  "airflow-compute")

(def ansible-local-tool "airflow-ansible-local")
(def ansible-remote-tool "airflow-ansible-remote")

(def github-tool
  "Where the seeded repository contents are rendered.

  A stage directory that never runs a tool: it exists so the workflow and the
  hello-world DAG are artifacts `golden.sh` can diff, rather than strings built
  inside the step that pushes them. What gets pushed is read back out of this
  directory, so the bytes the golden holds still are the bytes GitHub receives."
  "airflow-github")

(def dns-tool
  "ONCE's DNS stage directory, named here only so the backend advice and
  `golden.sh` can say it out loud. Not a choice this package gets to make."
  "tofu-dns")

(def smtp-tool "tofu-smtp")
(def smtp-post-tool "tofu-smtp-post")

;; ---------------------------------------------------------------------------
;; templates

(def ^:private airflow-root "io.github.getcolors.airflow.tools")
(def ^:private once-root "io.github.getcolors.once.tools")

(def ^:private template-opts
  "Selmer reads `<{ var }>` and `<% if %>`, leaving `{{ }}` and `{% %}` to
  Jinja2 in the Ansible files and to Go templates in nothing at all."
  sc/preserve-jinja-delimiters)

(defn tool-dir
  "The isolated working directory for `tool` in the active profile.

  A relative workdir resolves against the directory holding colors.yml rather
  than the current one, so a build renders to the same place however deep in the
  project it was invoked from.

  Identical to ONCE's but for the fallback profile name, which validation makes
  unreachable — `profile` is required."
  [opts tool]
  (green-cli/stage-dir opts tool {:default-profile "airflow"}))

(defn delegated-tool-dir
  "The working directory an ONCE step will choose for itself.

  Routed through ONCE's own `tool-dir` rather than reimplemented, so the backend
  advice writes `backend.tf.json` into exactly the directory the delegated step
  then runs in. Two copies of this resolution agreeing today is not the same as
  them agreeing after a pin bump."
  [opts tool]
  (once-tools/tool-dir opts tool))

(defn- once-template
  [tool provider file]
  (keyword (str once-root "." tool "." provider) file))

(defn- airflow-template
  [tool file]
  (keyword (str airflow-root "." tool) file))

(defn- template-spec
  [template target data]
  {:template template :target target :data data :opts template-opts})

(defn raw-spec [target content]
  (sc/content-spec target content))

;; ---------------------------------------------------------------------------
;; credentials

(defn credential-env
  "Environment additions for the providers in `slots`, plus the state backend —
  every stage reads and writes state, so the backend credentials belong to all
  of them. Unset credentials are omitted, so build and dry-run stay
  credential-free."
  [opts & slots]
  (provider-ops/tool-env validate/providers opts
                         (conj (vec slots) :provider-backend)))

(defn backend-credential-env
  "Environment additions for a process that only reads OpenTofu state. Provider
  credentials are left out: reading state never calls a provider API."
  [opts]
  (credential-env opts))

;; ---------------------------------------------------------------------------
;; compute

(defn fallback-compute-params
  "What a build or a dry-run stands in for the values only a real apply knows.

  Rendering must never need state, or `build` would stop being credential-free
  — and the DNS stage interpolates the address, so without this a build would
  have to reach for OpenTofu output before it could render a zone file.

  The shapes match ONCE's, because the delegated DNS step consumes them."
  [{:keys [profile provider-compute] :as opts}]
  (let [name (or profile "airflow")]
    (case provider-compute
      "azure" {:ip "192.168.0.1" :sudoer "ubuntu" :uid "1000" :name name :user "ubuntu"}
      "aws" {:ip "192.168.0.1" :sudoer "ubuntu" :uid "1000" :name name :user "ubuntu"}
      "google" {:ip "192.168.0.1" :sudoer "ubuntu" :uid "1000" :name name :user "ubuntu"}
      "digitalocean" {:ip "192.168.0.1" :sudoer "root" :name name :user "root"}
      "hcloud" {:ip "192.168.0.1" :sudoer "root" :name name :user "root"}
      "oci" {:ip "192.168.0.1" :sudoer "ubuntu" :uid "1001" :name name :user "ubuntu"}
      "yandex" {:ip "192.168.0.1" :sudoer "ubuntu" :uid "1000" :name name :user "ubuntu"}
      "no-infra" (cond-> {:ip (or (:no-infra-compute-ip opts) "192.168.0.1")
                          :sudoer (or (:no-infra-compute-sudoer opts) "root")
                          :name name
                          :user (or (:no-infra-compute-user opts) "root")}
                   (:no-infra-compute-uid opts) (assoc :uid (:no-infra-compute-uid opts)))
      {:ip "192.168.0.1" :sudoer "root" :name name :user "root"})))

(defn cidr-list
  "The CIDRs under `k`, trimmed and with blanks dropped.

  A YAML list is the shape colors.yml wants. A plain string is accepted too, and
  not as a convenience: `green.cli/read-pars` overlays `COLORS_PAR_*` onto flat
  keys as strings, so without this, setting `COLORS_PAR_DIGITALOCEAN_SSH_SOURCES`
  would replace the vector with a string and render one impossible CIDR — which
  OpenTofu would reject only once the apply reached the provider.

  Defaults to the whole internet rather than to nothing. An empty list renders a
  firewall rule that admits no one, and a machine you cannot reach is a worse
  failure than the open port the default already documents."
  [opts k]
  (let [v (get opts k)
        xs (if (sequential? v) v (str/split (str v) #"[,\s]+"))
        xs (->> xs (map (comp str/trim str)) (remove str/blank?) vec)]
    (if (seq xs) xs ["0.0.0.0/0" "::/0"])))

(defn firewall?
  "Whether to render the firewall beside ONCE's compute template.

  DigitalOcean only, because `firewall.tf` names `digitalocean_droplet.node1`.
  On any other provider the key is accepted and ignored rather than refused: the
  answer to \"is there a firewall\" is then the provider's own, and refusing
  would make a shared colors.yml unportable for no gain."
  [opts]
  (and (= "digitalocean" (:provider-compute opts))
       (not (false? (:digitalocean-firewall opts)))))

(defn compute-specs
  "ONCE's provider template, plus — on DigitalOcean — this package's firewall.

  OpenTofu merges every .tf in a directory, so the firewall needs no change to
  ONCE's template and no fork of it. That is the same trick walter uses to
  publish an instance id, and it is the reason the reuse survives at all.

  The address `firewall.tf` names, `digitalocean_droplet.node1`, is ONCE's;
  `scripts/golden.sh` asserts it is still declared upstream, because a rename
  there would otherwise surface as an opaque `tofu validate` failure during a
  real apply."
  [opts dir]
  (let [provider (or (:provider-compute opts) "digitalocean")
        data (assoc opts
                    :ssh-sources-hcl (tofu/hcl-list (cidr-list opts :digitalocean-ssh-sources))
                    :http-sources-hcl (tofu/hcl-list (cidr-list opts :digitalocean-http-sources)))]
    (cond-> [(template-spec (once-template "tofu" provider "main.tf")
                            (str dir "/main.tf")
                            data)]
      (firewall? opts)
      (conj (template-spec (airflow-template (str "tofu." provider) "firewall.tf")
                           (str dir "/firewall.tf")
                           data)))))

(defn- output-params
  [opts]
  (some-> (get-in opts [:tofu/outputs :params]) walk/keywordize-keys))

(defn compute-step
  "Render the compute stage and apply it, adopting the machine's address.

  The adopted params are merged flat into opts *and* kept under
  `:once/compute-params`. Both matter and for different reasons: the Ansible
  stage reads `ip` and `user` directly, and ONCE's delegated `tofu-dns-step`
  reads the namespaced key through its own `joined-params`. Dropping the
  namespaced one would silently point every DNS record at the fallback
  192.168.0.1 — a create that succeeds and resolves nowhere."
  [opts]
  (let [dir (tool-dir opts compute-tool)
        specs (compute-specs opts dir)
        fallback (fallback-compute-params opts)
        result (tofu/tofu-with-spec opts specs
                                    {:dir dir
                                     :env (credential-env opts :provider-compute)})]
    (cond
      (wf/failed? result) result
      (= :build (:green/event opts)) (merge result fallback {:once/compute-params fallback})
      ;; A destroy has run; there are no outputs left to adopt.
      (= :delete (:green/event opts)) result
      :else (let [params (merge fallback (output-params result))]
              (merge result params {:once/compute-params params})))))

;; ---------------------------------------------------------------------------
;; the three delegated stages
;;
;; Each is a one-line adapter: put desired state into the shape ONCE's step
;; reads, then hand it over. The adapters exist so `airflow-host` can stay a
;; flat key in this package's own configuration surface — an upstream rename
;; then costs one line here rather than an edit to every consumer's colors.yml.

(defn smtp-step
  "Register `notifications.<zone>` at Resend, via ONCE.

  Runs before DNS on create because the DNS stage publishes the verification
  records this stage returns, and after it on delete for the same reason in
  reverse."
  [opts]
  (once-tools/tofu-smtp-step (utils/with-once-shape opts)))

(defn dns-step
  "Point the host at the machine and publish the SMTP verification records, via
  ONCE.

  ONCE's step reads the address out of `:once/compute-params`, which
  `compute-step` above publishes. It also manages the zone's settings — which is
  why this package must not share a zone with another project running the same
  templates. See plans/0001-airflow-v1.md, \"Why a separate zone\"."
  [opts]
  (once-tools/tofu-dns-step (utils/with-once-shape opts)))

(defn smtp-post-step
  "Verify the Resend domains now that DNS resolves, via ONCE."
  [opts]
  (once-tools/tofu-smtp-post-step (utils/with-once-shape opts)))

;; ---------------------------------------------------------------------------
;; what the machine looks like
;;
;; These are constants rather than desired state on purpose. Every one of them
;; is a path or an identifier the package itself chose and nothing outside it
;; consumes, so making them configurable would widen the surface `golden.sh` has
;; to hold still without giving anyone a decision worth making. `dags-dest` is
;; the exception and it *is* desired state, because the rrsync ForceCommand and
;; the compose bind-mount both name it and CI has to know where it points.

(def airflow-conf-dir
  "Where the compose file and the two 0600 environment files live."
  "/etc/airflow")

(def airflow-logs-dir
  "Task logs, written by the containers.

  Under /var/lib rather than beside the DAGs: this is variable data the machine
  produces, not content pushed to it, and a restore has no interest in it."
  "/var/lib/airflow/logs")

(def walg-conf-dir "/etc/wal-g.d")

(def rrsync-path
  "Where the ForceCommand looks for rrsync.

  Ubuntu ships it as /usr/bin/rrsync, so this is a symlink the playbook makes
  rather than a copy. The indirection earns its keep: the authorized_keys line
  is rendered on the workstation, where the machine's package layout is not
  known, and a distro that moves the binary then costs one task on the box
  rather than a changed artifact and a re-published key."
  "/usr/local/bin/rrsync")

(def docker-bridge-gateway
  "The address Postgres accepts container connections on.

  Pinned in /etc/docker/daemon.json rather than accepted from Docker's own
  allocation. Left to itself the address is stable in practice and unpinned in
  principle, and Postgres does not start at all when it cannot bind a configured
  address — so the failure mode is a first boot that looks like a broken
  database rather than a moved bridge."
  "172.17.0.1")

(def airflow-uid
  "The uid the apache/airflow image's own user has. The logs directory is
  chowned to it, and the containers run as it rather than as root."
  "50000")

(def ^:private resend-relay
  "Resend's relay is identical for every account, so it is not desired state —
  the same call ONCE makes, and this is a copy rather than a reuse because
  ONCE's is private and a five-line map is not worth a seventh item on the reuse
  surface."
  {:smtp-server "smtp.resend.com"
   :smtp-port 587
   :smtp-username "resend"})

(defn smtp-relay
  "Host, port and username for the relay the machine sends alerts through.

  Airflow speaks SMTP directly with smtplib and so does the backup notifier, so
  there is no local MTA and these are the only mail settings on the box."
  [opts]
  (if (= "no-infra" (str (:provider-smtp opts)))
    {:smtp-server (:no-infra-smtp-server opts)
     :smtp-port (:no-infra-smtp-port opts)
     :smtp-username (:no-infra-smtp-username opts)}
    resend-relay))

(def ^:private smtp-password-keys
  {"resend" :resend-password
   "no-infra" :no-infra-smtp-password})

(defn par-lookup
  "The Jinja expression resolving a credential from the one parameter namespace
  every package in this stack shares.

  Evaluated by Ansible at play time on the controller, so the value reaches the
  machine without ever passing through the rendered work directory. Only the
  relay password needs computing — it is the one credential whose key depends on
  which provider was chosen — and the rest are written literally in the playbook
  where they can be read.

  **Every template interpolating this must use `|safe`.** Selmer HTML-escapes a
  substituted variable, and this expression is full of single quotes, so without
  it the playbook renders `lookup(&#39;env&#39;,&#39;…&#39;)` — which Ansible
  then fails to parse with `unexpected char '&'`. That is not hypothetical: it
  is how the first real create failed, thirty-four tasks in, on a machine that
  had already installed Docker, Postgres and WAL-G.

  The credentials written literally in the playbook never hit this, because
  nothing substitutes them. This one does precisely because it is computed."
  [k]
  (format "{{ lookup('env','COLORS_PAR_%s') }}"
          (-> (name k) (str/replace "-" "_") str/upper-case)))

(defn walg-prefix
  "The WAL-G object prefix, scoped by profile inside the bucket.

  The same reasoning as the OpenTofu state key one directory up: two projects
  may share a bucket, and the profile is what keeps one from replaying the
  other's WAL onto its own database."
  [opts]
  (format "s3://%s/%s"
          (str (:walg-r2-bucket opts))
          (or (:profile opts) "airflow")))

(defn pgdata
  "Debian's data directory for the pinned major version. `postgres-version` is
  validated as an integer, so this cannot render a path with a space in it."
  [opts]
  (format "/var/lib/postgresql/%s/main" (str (:postgres-version opts))))

(defn postgres-conf-dir
  [opts]
  (format "/etc/postgresql/%s/main" (str (:postgres-version opts))))

;; ---------------------------------------------------------------------------
;; the deploy key

(defn authorized-key-line
  "One authorized_keys line, confining the key to writing under `dags-dest`.

  This is where the design diverges from ONCE and it is the security crux.
  ONCE's forced command **ignores** `SSH_ORIGINAL_COMMAND`: the client sends
  nothing and all authority comes from this line. rsync cannot work that way —
  rsync-over-SSH runs `rsync --server` on the remote, so a forced command that
  permits rsync has to parse and validate what the client asked for. That is
  strictly weaker, and hand-rolling the validator is how people get owned:
  `-e`, `--rsh`, `--daemon` and protocol-level path handling all have to be
  refused correctly.

  So the forced command is `rrsync`, which ships with rsync, is maintained
  upstream and exists for exactly this. `-wo` makes it write-only, so a leaked
  key cannot read the DAGs back out, and the directory argument is the whole of
  its authority. It needs no sudo at all, unlike ONCE's key — Airflow's
  dag-processor rescans on a timer, so nothing has to be restarted after a sync.

  `restrict` is the second half: no pty, no agent forwarding, no port
  forwarding, no X11."
  [opts public]
  (format "restrict,command=\"%s -wo %s\" %s"
          rrsync-path
          (str (:dags-dest opts))
          public))

(defn deploy-keys-content
  "The authorized_keys lines for the current generation.

  One line, because this package has one repository. Pure and deterministic:
  this is rendered into an artifact `golden.sh` diffs, which is also why the key
  comment holds no timestamp."
  [opts]
  (let [lines (map #(authorized-key-line opts (:public %)) (github/public-keys opts))]
    (if (seq lines) (str (str/join "\n" lines) "\n") "")))

;; ---------------------------------------------------------------------------
;; the inventory and the template data

(defn inventory
  "The Ansible inventory for the one machine this package manages.

  ONCE's builder carries an admin/users split and a `root@host` key convention
  that a single-machine package has no use for, so this is walter's shape: one
  host, one group, keyed by the alias you would `ssh` with."
  [{:keys [ip user host-alias]}]
  (json/generate-string
   {:all {:hosts {(or host-alias "airflow") {:ansible_host ip :ansible_user user}}}}
   {:pretty true}))

(defn data-fn
  "Template data for the Ansible stages and the seed.

  `opts`, with the address and login guaranteed present so a build renders
  without ever reaching for state, plus everything derived: the machine layout
  above, the relay, and the paths built from `postgres-version`. Derived here
  rather than in the templates because three files interpolate `pgdata` and a
  fourth interpolates the directory beside it, and a template is the wrong place
  for an expression that has to agree with three others."
  [opts]
  (let [relay (smtp-relay opts)]
    (merge opts
           relay
           {:ip (or (not-empty (str (:ip opts))) "192.168.0.1")
            :user (or (not-empty (str (:user opts))) "root")
            :host-alias (utils/host-alias opts)
            :deploy-user github/deploy-user
            :airflow-conf-dir airflow-conf-dir
            :airflow-logs-dir airflow-logs-dir
            :airflow-uid airflow-uid
            :walg-conf-dir walg-conf-dir
            :walg-env-file (str walg-conf-dir "/env")
            :walg-prefix (walg-prefix opts)
            :rrsync-path rrsync-path
            :docker-bridge-gateway docker-bridge-gateway
            :pgdata (pgdata opts)
            :postgres-conf-dir (postgres-conf-dir opts)
            ;; The one credential whose COLORS_PAR_ name depends on a provider
            ;; choice, so it is computed rather than written into the playbook.
            ;;
            ;; Two forms, and the difference matters. The lookup expression is
            ;; for a file's content, where Ansible resolving it produces the
            ;; value. The bare variable name is for the preflight assertion,
            ;; which tests `lookup('env', '<name>') | length > 0` — rendering
            ;; the expression there would substitute the password into the
            ;; assertion's own text before the assertion was evaluated, which
            ;; both breaks on a password containing a quote and puts it
            ;; somewhere a failure message could print it.
            :smtp-password-par (green-cli/par-name
                                (get smtp-password-keys
                                     (str (:provider-smtp opts))
                                     :resend-password))
            :smtp-password-lookup (par-lookup (get smtp-password-keys
                                                   (str (:provider-smtp opts))
                                                   :resend-password))})))

;; ---------------------------------------------------------------------------
;; the seed

(def seed-files
  "What a newly created DAG repository is given, once.

  Rendered under the github stage rather than assembled in `github.clj` so both
  are artifacts, and pushed by reading these files back — the bytes `golden.sh`
  holds still are then the bytes GitHub receives."
  [{:path ".github/workflows/deploy-dags.yml" :template "deploy-dags.yml"}
   {:path "dags/hello_world.py" :template "hello_world.py"}])

(defn seed-specs
  [opts dir]
  (let [data (data-fn opts)]
    (mapv (fn [{:keys [path template]}]
            (template-spec (airflow-template "github" template)
                           (str dir "/seed/" path)
                           data))
          seed-files)))

(defn seed-step
  "Render the seed tree and attach it for the github step to push.

  Separate from `github.clj` because that namespace renders nothing and this
  one owns every template spec; the workflow composes the two. Reading the
  rendered files back rather than passing the strings along is deliberate — it
  makes the golden the authority on what gets pushed."
  [opts]
  (let [dir (tool-dir opts github-tool)
        rendered (sc/scaffold opts (seed-specs opts dir))]
    (cond
      (wf/failed? rendered) rendered

      ;; A delete scaffold REMOVES the rendered tree rather than writing it, so
      ;; there is nothing left to read back — and nothing to read it for, since
      ;; delete never seeds. Reading anyway is not a hypothetical mistake: it is
      ;; what the first real delete did, throwing FileNotFoundException out of
      ;; the very first stage and leaving the deploy credentials unrevoked.
      ;;
      ;; The scaffold still runs, because removing the rendered tree is the
      ;; useful half of what this step does on a delete.
      (= :delete (:green/event opts)) rendered

      :else
      (assoc rendered :airflow/seed-files
             (mapv (fn [{:keys [path]}]
                     {:path path :content (slurp (str dir "/seed/" path))})
                   seed-files)))))

;; ---------------------------------------------------------------------------
;; the Ansible stages

(defn ansible-local-step
  "Manage the `Host <alias>` block in `~/.ssh/config` on the workstation.

  This package's own copy rather than ONCE's, for walter's reason: ONCE's
  playbook writes to the operator's home directory, and reusing it would mean an
  unrelated change there — an added IdentityFile, a ProxyJump — rewriting that
  file at pin-bump time. Three small files against that exposure is a trade
  worth making even when everything else here delegates.

  The playbook's variables are Ansible's, not Selmer's, so they arrive as
  extra-vars: the local inventory targets localhost only and carries no host
  vars. `name` is reserved in Ansible, hence host_alias. block_state drives
  blockinfile in both directions, so a delete removes what a create wrote."
  [opts]
  (let [dir (tool-dir opts ansible-local-tool)
        data (data-fn opts)
        specs [(template-spec (airflow-template "ansible-local" "ansible.cfg")
                              (str dir "/ansible.cfg") data)
               (template-spec (airflow-template "ansible-local" "inventory.ini")
                              (str dir "/inventory.ini") data)
               (template-spec (airflow-template "ansible-local" "main.yml")
                              (str dir "/main.yml") data)]
        delete? (= :delete (:green/event opts))
        config {:dir dir
                :inventory "inventory.ini"
                :playbooks {:create "main.yml" :delete "main.yml"}
                :extra-vars {:host_alias (:host-alias data)
                             :ip (:ip data)
                             :user (:user data)
                             :block_state (if delete? "absent" "present")}}]
    (ansible/ansible-with-spec opts config specs)))

(def ^:private ansible-remote-files
  "Everything copied to the machine verbatim.

  `copy` rather than `template` on the far side, so Ansible does not render
  these a second time: they have already been through Selmer here, and a stray
  `{{ }}` in a shell script would otherwise be a Jinja expression rather than
  the text it looks like."
  ["authorized-keys"
   "docker-compose.yml"
   "Caddyfile"
   "wal-g-wrapper"
   "walg-basebackup"
   "walg-check"
   "walg-notify"])

(defn ansible-remote-specs
  [opts dir]
  (let [data (data-fn opts)]
    (into [(template-spec (airflow-template "ansible-remote" "ansible.cfg")
                          (str dir "/ansible.cfg") data)
           (template-spec (airflow-template "ansible-remote" "main.yml")
                          (str dir "/main.yml") data)
           (raw-spec (str dir "/inventory.json") (inventory data))
           (raw-spec (str dir "/deploy_keys") (deploy-keys-content opts))]
          (map (fn [file]
                 (template-spec (airflow-template "ansible-remote" (str "files/" file))
                                (str dir "/files/" file)
                                data)))
          ansible-remote-files)))

(defn ansible-remote-step
  "Provision the machine: Docker, Postgres, WAL-G, Airflow, Caddy and the deploy
  account.

  Renders and stops for a build or a delete. There is no cleanup playbook on the
  far side — a delete destroys the droplet, so reconciling anything on it first
  would be work against a machine about to stop existing. What delete does reach
  is the workstation, and that is `ansible-local`'s job."
  [opts]
  (let [dir (tool-dir opts ansible-remote-tool)
        rendered (sc/scaffold opts (ansible-remote-specs opts dir))]
    (if (or (= :build (:green/event opts))
            (= :delete (:green/event opts)))
      rendered
      (ansible/ansible-step rendered {:dir dir
                                      :inventory "inventory.json"
                                      :playbooks {:create "main.yml"}
                                      :host-key-checking false}))))
