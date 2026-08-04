(ns io.github.getcolors.airflow.validate
  "This package's desired-state rules, driven by ONCE's provider registry.

  The registry is consumed as data rather than reimplemented. It is the single
  place recording, per provider, the non-secret keys its templates interpolate
  and the credentials it needs, and keeping one copy is what stops a provider
  being validated against one set of keys and run with another.

  Unlike walter, which drives it over two slots, this package fills all four:
  it provisions a machine, stores state, manages DNS and sends mail.

  Nothing upstream promises this registry's shape. `scripts/golden.sh` is what
  actually catches a change to it — see plans/0001-airflow-v1.md."
  (:require
   [clojure.string :as str]
   [green.cli :as green-cli]
   [io.github.getcolors.once.validate :as once-validate]))

(def providers
  "ONCE's provider registry, verbatim."
  once-validate/providers)

(def slots
  "Every provider slot ONCE defines. This package fills all four."
  [:provider-compute :provider-smtp :provider-dns :provider-backend])

(def stoppable
  "Compute providers this package can power cycle.

  Empty in v1, which is a statement rather than an oversight: walter implements
  the power verbs for OCI only, and this package's first consumer runs on
  DigitalOcean. An Airflow scheduler runs continuously, so a droplet that cannot
  be parked costs nothing that was not already being paid. DigitalOcean has a
  power API and adding it later is additive.

  Membership is a fact about the provider's API, not about its OpenTofu
  template — power state is never declared in any template, so stopping a
  machine out of band causes no drift."
  #{})

(defn stoppable?
  [opts]
  (contains? stoppable (str (:provider-compute opts))))

(def own-required
  "Non-secret keys this package's own templates interpolate, on top of whatever
  the selected providers require.

  Every one of these reaches a rendered file. A key that only appeared in a
  comment would not belong here."
  [:airflow-host :airflow-image :airflow-admin-username
   :caddy-image
   :airflow-smtp-from
   :dags-repo :dags-dest :dags-branch
   :postgres-version
   :walg-version
   :walg-r2-bucket :walg-r2-endpoint :walg-r2-region
   :walg-full-backup-oncalendar :walg-retain-full :walg-max-backup-age-hours
   :alerts-email])

(def own-secrets
  "Credentials this package needs that no provider entry declares.

  `:github-token` is here rather than in the registry because ONCE treats it the
  same way — it belongs to a service outside the four provider slots. The rest
  are the ones that reach the machine, which is the way this package departs
  from walter: walter puts no secret on a server at all."
  [:github-token
   :postgres-password :airflow-fernet-key :airflow-admin-password
   :walg-r2-access-key-id :walg-r2-secret-access-key])

(defn- entry
  [opts slot]
  (get-in providers [slot (get opts slot)]))

(defn tofu-env
  "Flat key -> the environment variable OpenTofu reads it from, for the provider
  selected in `slot`."
  [opts slot]
  (:tofu-env (entry opts slot) {}))

(defn- slot-keys
  [opts field]
  (mapcat #(get (entry opts %) field []) slots))

(defn placeholder?
  "Whether a value is missing in the ways a hand-edited file produces: absent,
  blank, or still carrying the scaffold's REPLACE_ME."
  [x]
  (or (nil? x)
      (and (string? x)
           (or (str/blank? x)
               (= "REPLACE_ME" (str/upper-case x))))))

(defn- missing-keys
  [opts ks]
  (keep (fn [k] (when (placeholder? (get opts k)) k)) ks))

(def ^:private gated-keys
  "Optional keys a template interpolates behind an `<% if key|not-empty %>` gate.

  Deliberately a list rather than a scan of every unrequired key. A colors.yml
  carries REPLACE_ME for every provider it is *not* using, and those are
  harmless because the template that would read them is never rendered. These
  are the ones where a placeholder is not harmless: the gate fires and the
  placeholder reaches the generated file verbatim."
  [:caddy-acme-email :digitalocean-vpc-uuid :oci-image-id])

(defn- leftover-placeholders
  [opts]
  (for [k gated-keys
        :when (and (contains? opts k)
                   (placeholder? (get opts k))
                   (some? (get opts k))
                   (not (str/blank? (str (get opts k)))))]
    (str k " still says REPLACE_ME — fill it in, or delete the key. "
         "An optional key is not treated as absent while it holds a "
         "placeholder: it renders into the generated files verbatim.")))

(def profile-par
  "The one `COLORS_PAR_*` variable this package refuses to honour."
  (green-cli/par-name :profile))

(defn env-errors
  "Errors that depend on the environment rather than on the file.

  `profile` names the work directory and the OpenTofu state keys, and the
  project it identifies is the directory holding colors.yml. An override from
  the environment can therefore only point this package at another project's
  state — in this stack, plausibly at one running a production website from the
  same R2 bucket.

  Rejected outright rather than checked against an expected value:
  `green.cli/read-pars` has already overwritten the file's value by the time any
  step runs, so there is nothing left to compare against."
  [env]
  (when (not-empty (str (get env profile-par)))
    [(str profile-par " is set. This package takes its profile from colors.yml "
          "only — run from the project directory rather than overriding it.")]))

(def ^:private host-re
  #"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$")
(def ^:private repo-re #"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
(def ^:private email-re #"^[^@\s]+@[^@\s]+\.[^@\s]+$")
(def ^:private abs-path-re #"^/\S*$")
(def ^:private release-tag-re #"^v?\d+(?:\.\d+)*(?:[-.][A-Za-z0-9.]+)?$")

(defn- positive-int?
  [x]
  (and (integer? x) (pos? x)))

(defn state-errors
  "Everything wrong with `opts` that does not depend on credentials, as a vector
  of messages. Empty means the desired state is renderable."
  [opts]
  (vec
   (concat
    (map #(str % " is required")
         (missing-keys opts (concat [:profile :workdir]
                                    own-required
                                    (slot-keys opts :required))))
    (leftover-placeholders opts)

    (for [slot slots
          :let [provider (get opts slot)]
          :when (not (contains? (get providers slot) provider))]
      (str "unsupported " slot " " (pr-str provider)))

    (when-not (boolean? (:compute-prevent-destroy opts))
      [":compute-prevent-destroy must be true or false"])

    ;; The host is a DNS name, a Caddy site address and half of a Resend sending
    ;; domain. A malformed one fails three stages in, against live DNS.
    (when-not (or (placeholder? (:airflow-host opts))
                  (re-matches host-re (str (:airflow-host opts))))
      [":airflow-host must be a fully qualified hostname"])

    ;; owner/name. Checked here because the github stage runs LAST on create —
    ;; a typo would otherwise surface after the droplet, DNS and Ansible have
    ;; all succeeded.
    (when-not (or (placeholder? (:dags-repo opts))
                  (re-matches repo-re (str (:dags-repo opts))))
      [":dags-repo must be owner/name"])

    ;; rrsync confines the deploy key to this directory, so a relative path
    ;; would confine it relative to the login user's home — which is not what
    ;; the ForceCommand line says, and not what the compose file bind-mounts.
    (when-not (or (placeholder? (:dags-dest opts))
                  (re-matches abs-path-re (str (:dags-dest opts))))
      [":dags-dest must be an absolute path"])

    (for [[k label] [[:alerts-email "alert"]
                     [:airflow-smtp-from "sender"]]
          :when (and (not (placeholder? (get opts k)))
                     (not (re-matches email-re (str (get opts k)))))]
      (str k " must be an email address (the " label " address)"))

    ;; Airflow will not start with a floating tag pinned to nothing, and an
    ;; unpinned one makes two creates months apart different deployments.
    (when-not (or (placeholder? (:airflow-image opts))
                  (str/includes? (str (:airflow-image opts)) ":"))
      [":airflow-image must carry an explicit tag — a floating tag makes two "
       "creates different deployments, and an Airflow minor upgrade migrates "
       "the metadata database"])

    ;; Same rule, and it applies with more force rather than less: this is the
    ;; one process on the machine facing the internet, and an unpinned tag would
    ;; make it the only unpinned thing on it.
    (when-not (or (placeholder? (:caddy-image opts))
                  (str/includes? (str (:caddy-image opts)) ":"))
      [":caddy-image must carry an explicit tag"])

    ;; WAL-G backups do not restore across Postgres major versions, so this pin
    ;; is what stands between a distro upgrade and an unrestorable archive.
    (when-not (positive-int? (:postgres-version opts))
      [":postgres-version must be a positive integer major version"])

    ;; Interpolated into a GitHub release download URL and into the .sha256 URL
    ;; beside it. Anything with a space or a slash in it builds a URL that 404s
    ;; on the machine, half way through a create, rather than failing here.
    (when-not (or (placeholder? (:walg-version opts))
                  (re-matches release-tag-re (str (:walg-version opts))))
      [(str ":walg-version must be a WAL-G release tag, e.g. "
            (pr-str "v3.0.8") " — it names a GitHub release asset")])

    (when-not (positive-int? (:walg-retain-full opts))
      [":walg-retain-full must be a positive integer"])

    ;; Below the backup interval this alerts every cycle; the file's own comment
    ;; works the arithmetic. Only the degenerate case is enforced here, because
    ;; the interval is an OnCalendar expression this package does not parse.
    (when-not (positive-int? (:walg-max-backup-age-hours opts))
      [":walg-max-backup-age-hours must be a positive integer"])

    ;; A systemd OnCalendar expression, not a crontab line. Five whitespace-
    ;; separated fields is the shape of the crontab someone reaches for out of
    ;; habit, and systemd would reject it on the machine rather than here.
    (let [cal (str (:walg-full-backup-oncalendar opts))]
      (when (and (not (placeholder? cal))
                 (= 5 (count (str/split (str/trim cal) #"\s+")))
                 (not (str/includes? cal ":")))
        [(str ":walg-full-backup-oncalendar looks like a crontab line. It is a "
              "systemd OnCalendar expression — daily at 02:00 is "
              (pr-str "*-*-* 02:00:00") ", not " (pr-str "0 2 * * *"))]))

    ;; The sender has to sit under the Resend sending domain, which ONCE derives
    ;; as notifications.<zone> from the application host. A From address on the
    ;; bare zone is not what gets verified, and Resend rejects mail from it.
    (let [from (str (:airflow-smtp-from opts))
          host (str (:airflow-host opts))]
      (when (and (= "resend" (:provider-smtp opts))
                 (not (placeholder? from))
                 (not (placeholder? host))
                 (re-matches email-re from)
                 (not (str/ends-with? from (str "@notifications."
                                                (let [ls (str/split host #"\.")]
                                                  (str/join "." (take-last 2 ls)))))))
        [(str ":airflow-smtp-from must be under the Resend sending domain "
              "notifications." (let [ls (str/split host #"\.")]
                                 (str/join "." (take-last 2 ls)))
              " — that subdomain is what gets verified, not the bare zone")])))))

(defn secret-errors
  "Credentials the selected providers and this package need that no
  `COLORS_PAR_*` variable supplied."
  [opts]
  (map #(str "required credential is not set: " (green-cli/par-name %))
       (distinct (missing-keys opts (concat own-secrets (slot-keys opts :secrets))))))
