(ns io.github.getcolors.airflow.utils
  "The launcher compatibility number and the pure helpers shared across steps."
  (:require
   [clojure.string :as str]))

(def contract
  "Minimum interface version a launcher must require to drive this library.

  Bump on any change a launcher pinned to an older commit could not survive — a
  renamed desired-state key, a changed template variable, a new function the
  launcher calls — and bump `launcher-contract` in the bundled launcher to
  match. The handshake turns a stale pin into an actionable exit 2 rather than a
  confusing resolution failure."
  1)

(defn host-alias
  "The `~/.ssh/config` Host alias this package manages.

  `profile` names the project, so it names the alias: `ssh <profile>` reaches
  the machine."
  [opts]
  (or (not-empty (str (:profile opts))) "airflow"))

(defn registrable-domain
  "The zone `host` sits in — the last two labels.

  ONCE has its own copy of this and derives DNS zones with it. Duplicated rather
  than reused because it is four lines, and because the version that matters for
  a state address should not move when ONCE's does."
  [host]
  (let [labels (str/split (str host) #"\.")]
    (if (< (count labels) 2)
      (str host)
      (str/join "." (take-last 2 labels)))))

(defn once-applications
  "Desired state in the shape ONCE's DNS and SMTP steps read.

  This is the whole adapter, and its narrowness is the point. `airflow-host` is
  a flat key in this package's own configuration surface; ONCE's steps want
  `[:once :applications]` with a `:host` on each entry. Assembling it here means
  an upstream rename is a one-line fix rather than a breaking change to every
  consumer's colors.yml.

  No `:image`, `:github` or `:env`: those drive ONCE's container deployment,
  which this package does not use. Only the host reaches the DNS and SMTP
  templates."
  [opts]
  {:applications [{:host (:airflow-host opts)}]})

(defn with-once-shape
  "`opts` plus the ONCE-shaped view of it, for handing to an ONCE step."
  [opts]
  (assoc opts :once (once-applications opts)))
