(ns io.github.getcolors.airflow.tools-test
  "The steps, the derived template data, and the couplings to ONCE that a
  golden diff cannot see."
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [green.cli :as green-cli]
   [green.tofu :as tofu]
   [io.github.getcolors.airflow.tools :as tools]
   [io.github.getcolors.airflow.validate-test :refer [fixture]]))

(defn- temp-dir
  []
  (str (java.nio.file.Files/createTempDirectory
        "airflow-test" (into-array java.nio.file.attribute.FileAttribute []))))

(defn- opts-in
  "The fixture, rendering into a throwaway absolute workdir."
  [event & {:as overrides}]
  (merge (fixture) {:green/event event :workdir (temp-dir)} overrides))

(defn- slurp-rendered
  [opts tool file]
  (slurp (str (tools/tool-dir opts tool) "/" file)))

;; ---------------------------------------------------------------------------
;; the compute stage

(deftest the-firewall-renders-for-digitalocean-only
  (testing "firewall.tf names digitalocean_droplet.node1, which no other
            provider's template declares"
    (is (tools/firewall? (fixture)))
    (doseq [provider ["hcloud" "oci" "yandex" "no-infra"]]
      (is (not (tools/firewall? (fixture :provider-compute provider)))
          (str provider " must render no firewall")))))

(deftest the-firewall-can-be-turned-off-but-not-on-elsewhere
  (is (not (tools/firewall? (fixture :digitalocean-firewall false))))
  (testing "the key is accepted and ignored on another provider rather than
            refused, so a shared colors.yml stays portable"
    (is (not (tools/firewall? (fixture :provider-compute "hcloud"
                                       :digitalocean-firewall true))))))

(deftest compute-specs-put-the-firewall-beside-ONCEs-template
  (let [specs (tools/compute-specs (fixture) "/tmp/x")]
    (is (= 2 (count specs)))
    (is (= "/tmp/x/main.tf" (:target (first specs))))
    (is (= "/tmp/x/firewall.tf" (:target (second specs))))
    (testing "ONCE's template is reached by classpath keyword, not forked"
      (is (= :io.github.getcolors.once.tools.tofu.digitalocean/main.tf
             (:template (first specs)))))))

(deftest cidr-lists-survive-a-parameter-override
  (testing "read-pars overlays COLORS_PAR_* onto flat keys as strings, so
            without this a COLORS_PAR_DIGITALOCEAN_SSH_SOURCES would replace the
            vector with one impossible CIDR that OpenTofu only rejects at apply"
    (is (= ["10.0.0.0/8" "192.0.2.0/24"]
           (tools/cidr-list {:k "10.0.0.0/8,192.0.2.0/24"} :k)))
    (is (= ["10.0.0.0/8" "192.0.2.0/24"]
           (tools/cidr-list {:k ["10.0.0.0/8" " 192.0.2.0/24 "]} :k))))
  (testing "and an empty list opens rather than closes: a machine you cannot
            reach is a worse failure than the open port the default documents"
    (is (= ["0.0.0.0/0" "::/0"] (tools/cidr-list {} :k)))
    (is (= ["0.0.0.0/0" "::/0"] (tools/cidr-list {:k []} :k)))))

;; ---------------------------------------------------------------------------
;; the undocumented contract with ONCE
;;
;; scripts/golden.sh cannot reach this, and the reason is worth stating: on a
;; build, this package's fallback compute params and ONCE's are the same map by
;; construction, so a rendered A record looks identical whether or not the key
;; is published. Only a create tells them apart, and only these tests run one.

(deftest compute-step-publishes-the-key-ONCEs-dns-step-reads
  (testing "ONCE's tofu-dns-step reads :once/compute-params. Its own compute
            step sets that key; this package's compute step is its own, so it
            must publish it too — dropping it would fall through to ONCE's
            fallback and point every A record at 192.168.0.1, a create that
            succeeds and resolves nowhere."
    (with-redefs [tofu/tofu-step
                  (fn [opts _] (assoc opts
                                      :green/exit 0
                                      :tofu/outputs {:params {"ip" "203.0.113.7"
                                                              "user" "root"
                                                              "sudoer" "root"
                                                              "name" "airflow"}}))]
      (let [result (tools/compute-step (opts-in :create))]
        (is (= "203.0.113.7" (:ip result))
            "the address is merged flat for the Ansible stages")
        (is (= "203.0.113.7" (get-in result [:once/compute-params :ip]))
            "and kept namespaced for ONCE's delegated DNS step")))))

(deftest ONCEs-dns-step-still-reads-that-key
  (testing "the other end of the same contract, and the one that breaks on a pin
            bump. The flat :ip is removed so the namespaced key is the only
            possible source — if ONCE has stopped reading it, this renders
            192.168.0.1 instead."
    (with-redefs [tofu/tofu-step (fn [opts _] (assoc opts :green/exit 0))]
      (let [opts (-> (opts-in :create)
                     (dissoc :ip)
                     (assoc :once/compute-params {:ip "203.0.113.7"
                                                  :user "root"
                                                  :sudoer "root"
                                                  :name "airflow"}))
            result (tools/dns-step opts)
            apps (slurp (str (tools/delegated-tool-dir result tools/dns-tool)
                             "/apps.tf.json"))]
        (is (str/includes? apps "203.0.113.7")
            "the A record must carry the address the compute stage published")
        (is (not (str/includes? apps "192.168.0.1"))
            "and not ONCE's fallback")))))

(deftest the-adapter-feeds-ONCEs-smtp-step
  (testing "airflow-host is a flat key here; ONCE's steps want
            [:once :applications] with a :host on each entry. One line in utils
            is the whole adapter, and this is what proves it still fits."
    (with-redefs [tofu/tofu-step (fn [opts _] (assoc opts :green/exit 0))]
      (let [result (tools/smtp-step (opts-in :create))
            main (slurp (str (tools/delegated-tool-dir result tools/smtp-tool)
                             "/main.tf"))]
        (is (str/includes? main "fixture.example")
            "the zone derived from airflow-host must reach ONCE's template")
        (is (str/includes? main "notifications"))))))

(deftest the-delegated-directories-are-ONCEs-own
  (testing "each ONCE step hard-codes its directory internally, so this package
            routes through ONCE's tool-dir rather than reimplementing it — the
            backend advice has to write into exactly the directory the step then
            runs in"
    (let [opts (fixture :workdir "/tmp/wd" :green/state-file "/tmp/p/colors.yml")]
      (is (str/ends-with? (tools/delegated-tool-dir opts tools/dns-tool)
                          "airflow-fixture/tofu-dns"))
      (is (str/ends-with? (tools/tool-dir opts tools/compute-tool)
                          "airflow-fixture/airflow-compute")))))

;; ---------------------------------------------------------------------------
;; the deploy key

(deftest the-force-command-confines-the-key-to-the-dag-directory
  (testing "rsync-over-SSH runs `rsync --server` on the remote, so a forced
            command that permits rsync has to validate what the client asked
            for. rrsync is what does that correctly."
    (let [line (tools/authorized-key-line (fixture) "ssh-ed25519 AAAA test-comment")]
      (is (str/starts-with? line "restrict,")
          "restrict denies pty, agent and port forwarding")
      (is (str/includes? line "command=\"/usr/local/bin/rrsync -wo /srv/airflow/dags\"")
          "-wo makes it write-only, and the directory is the whole authority")
      (is (str/ends-with? line "ssh-ed25519 AAAA test-comment"))
      (testing "and it needs no sudo at all, unlike ONCE's key"
        (is (not (str/includes? line "sudo")))))))

(deftest the-rendered-key-file-holds-only-the-current-generation
  (testing "retaining the previous one is the reconciler's job, on the box,
            where the previous key actually is"
    (let [opts (assoc (fixture) :airflow/deploy-keys
                      [{:public "ssh-ed25519 AAAA one"}])]
      (is (= 1 (count (str/split-lines (tools/deploy-keys-content opts)))))
      (is (str/ends-with? (tools/deploy-keys-content opts) "\n")))
    (is (= "" (tools/deploy-keys-content (fixture)))
        "no keys means an empty file, not a file with a blank line")))

;; ---------------------------------------------------------------------------
;; derived template data

(deftest the-machine-paths-come-from-the-pinned-version
  (let [data (tools/data-fn (fixture))]
    (is (= "/var/lib/postgresql/16/main" (:pgdata data)))
    (is (= "/etc/postgresql/16/main" (:postgres-conf-dir data)))
    (testing "and follow the pin rather than being written down twice"
      (is (= "/var/lib/postgresql/14/main"
             (:pgdata (tools/data-fn (fixture :postgres-version 14))))))))

(deftest the-backup-prefix-is-scoped-by-profile
  (testing "the same reasoning as the OpenTofu state key: two projects may share
            a bucket, and the profile is what keeps one from replaying the
            other's WAL onto its own database"
    (is (= "s3://airflow-fixture-backup/airflow-fixture"
           (tools/walg-prefix (fixture))))))

(deftest the-relay-is-hard-coded-for-resend-and-configurable-otherwise
  (testing "Resend's relay is identical for every account, so it is not desired
            state — only the password is"
    (is (= {:smtp-server "smtp.resend.com" :smtp-port 587 :smtp-username "resend"}
           (tools/smtp-relay (fixture))))
    (is (= {:smtp-server "smtp.fixture.example"
            :smtp-port 587
            :smtp-username "fixture"}
           (tools/smtp-relay (fixture :provider-smtp "no-infra"))))))

(deftest the-relay-password-key-follows-the-provider
  (testing "the one credential whose COLORS_PAR_ name depends on a provider
            choice, which is why it is computed rather than written down"
    (let [data (tools/data-fn (fixture))]
      (is (= "COLORS_PAR_RESEND_PASSWORD" (:smtp-password-par data)))
      (is (= "{{ lookup('env','COLORS_PAR_RESEND_PASSWORD') }}"
             (:smtp-password-lookup data))))
    (let [data (tools/data-fn (fixture :provider-smtp "no-infra"))]
      (is (= "COLORS_PAR_NO_INFRA_SMTP_PASSWORD" (:smtp-password-par data)))
      (is (= "{{ lookup('env','COLORS_PAR_NO_INFRA_SMTP_PASSWORD') }}"
             (:smtp-password-lookup data))))))

(deftest a-build-never-reaches-for-state
  (testing "the address and login are guaranteed present, or a build would have
            to read OpenTofu output before it could render a playbook"
    (let [data (tools/data-fn (dissoc (fixture) :ip :user))]
      (is (= "192.168.0.1" (:ip data)))
      (is (= "root" (:user data))))))

(deftest the-inventory-names-one-host-by-its-ssh-alias
  (let [parsed (json/parse-string (tools/inventory {:ip "203.0.113.7"
                                                    :user "root"
                                                    :host-alias "airflow-fixture"})
                                  true)]
    (is (= {:ansible_host "203.0.113.7" :ansible_user "root"}
           (get-in parsed [:all :hosts :airflow-fixture])))))

;; ---------------------------------------------------------------------------
;; the rendered playbook

(deftest no-credential-is-rendered-into-the-work-directory
  (testing "this package puts secrets on a server, which walter does not. They
            travel in the process environment and are written by Ansible into
            0600 files — never through .colors/. This is the check that a
            'simplification' interpolating one at render time would trip."
    (let [opts (opts-in :build)
          _ (tools/ansible-remote-step opts)
          playbook (slurp-rendered opts tools/ansible-remote-tool "main.yml")]
      (doseq [par ["COLORS_PAR_POSTGRES_PASSWORD"
                   "COLORS_PAR_AIRFLOW_FERNET_KEY"
                   "COLORS_PAR_AIRFLOW_ADMIN_PASSWORD"
                   "COLORS_PAR_WALG_R2_ACCESS_KEY_ID"
                   "COLORS_PAR_WALG_R2_SECRET_ACCESS_KEY"
                   "COLORS_PAR_RESEND_PASSWORD"]]
        (is (str/includes? playbook par)
            (str par " must be resolved by the playbook"))
        (is (re-find (re-pattern (str "lookup\\('env',\\s*'" par "'\\)")) playbook)
            (str par " must be an Ansible lookup, not a rendered value"))))))

(deftest the-remote-stage-renders-everything-it-copies
  (let [opts (opts-in :build)
        _ (tools/ansible-remote-step opts)
        dir (tools/tool-dir opts tools/ansible-remote-tool)]
    (doseq [file ["ansible.cfg" "main.yml" "inventory.json" "deploy_keys"
                  "files/authorized-keys" "files/docker-compose.yml"
                  "files/Caddyfile" "files/wal-g-wrapper" "files/walg-basebackup"
                  "files/walg-check" "files/walg-notify"]]
      (is (.exists (io/file dir file)) (str file " must be rendered")))
    (testing "and the playbook copies each script it references"
      (let [playbook (slurp (io/file dir "main.yml"))]
        (doseq [script ["wal-g-wrapper" "walg-basebackup" "walg-check" "walg-notify"]]
          (is (str/includes? playbook script)))))))

(deftest the-caddyfile-gates-the-acme-email
  (let [without (opts-in :build)
        with (opts-in :build :caddy-acme-email "ops@fixture.example")]
    (tools/ansible-remote-step without)
    (tools/ansible-remote-step with)
    (is (not (str/includes? (slurp-rendered without tools/ansible-remote-tool
                                            "files/Caddyfile")
                            "email "))
        "an absent optional key renders no global options block at all")
    (is (str/includes? (slurp-rendered with tools/ansible-remote-tool
                                       "files/Caddyfile")
                       "email ops@fixture.example"))))

(deftest the-api-server-publishes-no-port
  (testing "the other half of moving authentication to the edge: Caddy is not
            merely the recommended route to Airflow, it is the only one"
    (let [opts (opts-in :build)
          _ (tools/ansible-remote-step opts)
          compose (slurp-rendered opts tools/ansible-remote-tool
                                  "files/docker-compose.yml")
          caddyfile (slurp-rendered opts tools/ansible-remote-tool "files/Caddyfile")]
      (is (not (re-find #"(?m)^\s+- \"(127\.0\.0\.1:)?8080:8080\"" compose))
          "no published port for the api-server, not even one bound to localhost")
      (is (re-find #"(?m)^\s+- \"80:80\"" compose)
          "only Caddy publishes ports, and 80 is the ACME challenge")
      (testing "so the only route in is Caddy addressing it by service name"
        (is (str/includes? caddyfile "reverse_proxy airflow-apiserver:8080"))))))

;; ---------------------------------------------------------------------------
;; the seed

(deftest the-seed-is-rendered-and-read-back
  (testing "pushed by reading these files back rather than by passing strings
            along, so the golden is the authority on what GitHub receives"
    (let [opts (opts-in :build)
          result (tools/seed-step opts)
          files (:airflow/seed-files result)]
      (is (= [".github/workflows/deploy-dags.yml" "dags/hello_world.py"]
             (mapv :path files)))
      (is (every? #(seq (:content %)) files))
      (is (str/includes? (:content (first files)) "environment: airflow-fixture")
          "the workflow must name the profile's Actions environment")
      (is (str/includes? (:content (first files)) "rrsync")
          "and explain what it is syncing into")
      (doseq [{:keys [path content]} files]
        (is (= content (slurp (str (tools/tool-dir opts tools/github-tool)
                                   "/seed/" path)))
            "what is pushed must be exactly what was rendered")))))

;; ---------------------------------------------------------------------------
;; credentials into the process environment

(deftest credentials-travel-in-the-environment-and-only-when-set
  (testing "so build and dry-run stay credential-free"
    (is (nil? (tools/credential-env (fixture) :provider-compute)))
    (is (= {"DIGITALOCEAN_TOKEN" "tok"}
           (tools/credential-env (fixture :do-token "tok") :provider-compute)))
    (testing "and the backend's belong to every stage, since all of them read state"
      (is (= {"AWS_ACCESS_KEY_ID" "k" "AWS_SECRET_ACCESS_KEY" "s"}
             (tools/credential-env (fixture :provider-backend "r2"
                                            :r2-access-key-id "k"
                                            :r2-secret-access-key "s")))))))

(deftest the-par-lookup-expression-is-the-shared-one
  (testing "the same namespace every package in this stack uses; there is no
            per-package prefix"
    (is (= "{{ lookup('env','COLORS_PAR_POSTGRES_PASSWORD') }}"
           (tools/par-lookup :postgres-password)))
    (is (= "COLORS_PAR_POSTGRES_PASSWORD" (green-cli/par-name :postgres-password)))))
