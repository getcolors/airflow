(ns io.github.getcolors.airflow.validate-test
  "The desired-state rules, and the registry they are driven by."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [green.cli :as green-cli]
   [io.github.getcolors.airflow.validate :as validate]))

(def fixture-file "test/fixtures/colors.yml")

(defn fixture
  "The golden fixture, which is also the file scripts/golden.sh renders. Using
  one file for both means a rule added here without a value there fails loudly
  rather than being tested against a map only this namespace believes in."
  [& {:as overrides}]
  (merge (green-cli/read-state fixture-file (slurp fixture-file))
         overrides))

(defn- errors-matching
  [opts fragment]
  (filter #(str/includes? % fragment) (validate/state-errors opts)))

(deftest the-fixture-is-renderable
  (is (= [] (validate/state-errors (fixture)))
      "the fixture must validate cleanly, or every other test here is asserting
       against a map that could never be built"))

(deftest every-provider-slot-is-filled
  (testing "unlike walter, which drives the registry over two slots"
    (is (= [:provider-compute :provider-smtp :provider-dns :provider-backend]
           validate/slots))
    (is (every? #(contains? validate/providers %) validate/slots))))

(deftest an-unsupported-provider-is-named
  (doseq [slot validate/slots]
    (is (seq (errors-matching (fixture slot "nonesuch") "unsupported"))
        (str "a bad " slot " must be reported"))))

(deftest every-provider-in-the-registry-can-be-selected
  (testing "a provider the registry offers must render, or the registry is
            promising something this package cannot deliver"
    (doseq [provider (keys (get validate/providers :provider-compute))]
      (is (= [] (validate/state-errors (fixture :provider-compute provider)))
          (str "provider-compute " provider " should validate against the fixture")))
    (doseq [provider (keys (get validate/providers :provider-backend))]
      (is (= [] (validate/state-errors (fixture :provider-backend provider)))
          (str "provider-backend " provider " should validate against the fixture")))))

;; ---------------------------------------------------------------------------
;; the two keys this revision changed

(deftest walg-version-is-required
  (testing "the pin is what makes the archive readable by the binary that wrote
            it, so an absent one is not a default"
    (is (seq (errors-matching (dissoc (fixture) :walg-version) ":walg-version")))))

(deftest walg-version-must-look-like-a-release-tag
  (testing "it is interpolated into a GitHub release URL, so a malformed one
            404s on the machine half way through a create rather than failing
            here"
    (is (seq (errors-matching (fixture :walg-version "latest release") "release tag")))
    (is (seq (errors-matching (fixture :walg-version "v3.0.8/../..") "release tag")))
    (is (empty? (errors-matching (fixture :walg-version "v3.0.8") "release tag")))
    (is (empty? (errors-matching (fixture :walg-version "3.0.8") "release tag")))))

(deftest airflow-admin-email-is-not-required
  (testing "it named a mailbox nothing delivered to once authentication moved to
            Caddy, and a required key that reaches no rendered file makes the
            required list a lie"
    (is (not (contains? (set validate/own-required) :airflow-admin-email)))
    (is (= [] (validate/state-errors (dissoc (fixture) :airflow-admin-email))))))

;; ---------------------------------------------------------------------------
;; the rules that exist because of where they would otherwise fail

(deftest the-host-must-be-a-hostname
  (testing "it is a DNS name, a Caddy site address and half of a Resend sending
            domain — a malformed one fails three stages in, against live DNS"
    (is (seq (errors-matching (fixture :airflow-host "not a host") ":airflow-host")))
    (is (seq (errors-matching (fixture :airflow-host "airflow") ":airflow-host"))
        "a single label has no registrable domain to derive a zone from")))

(deftest the-dags-repo-must-be-owner-slash-name
  (testing "checked here because the github stage runs LAST on create — a typo
            would otherwise surface after compute, DNS and Ansible all succeeded"
    (is (seq (errors-matching (fixture :dags-repo "airflow-dags") ":dags-repo")))
    (is (seq (errors-matching (fixture :dags-repo "a/b/c") ":dags-repo")))))

(deftest the-dags-destination-must-be-absolute
  (testing "rrsync confines the deploy key to this directory, so a relative path
            would confine it relative to the login user's home — which is not
            what the ForceCommand line says or what compose bind-mounts"
    (is (seq (errors-matching (fixture :dags-dest "srv/airflow/dags") ":dags-dest")))))

(deftest images-must-carry-a-tag
  (testing "an unpinned tag makes two creates months apart different deployments"
    (is (seq (errors-matching (fixture :airflow-image "apache/airflow") ":airflow-image")))
    (is (seq (errors-matching (fixture :caddy-image "caddy") ":caddy-image")))))

(deftest postgres-version-must-be-an-integer
  (testing "WAL-G backups do not restore across major versions, so this pin is
            what stands between a distro upgrade and an unrestorable archive"
    (is (seq (errors-matching (fixture :postgres-version "16.2") ":postgres-version")))
    (is (seq (errors-matching (fixture :postgres-version 0) ":postgres-version")))))

(deftest a-crontab-line-is-caught-rather-than-shipped
  (testing "systemd would reject it on the machine rather than here, and the two
            syntaxes look close enough to be reached for out of habit"
    (let [errs (errors-matching (fixture :walg-full-backup-oncalendar "0 2 * * *")
                                "crontab")]
      (is (seq errs))
      (is (str/includes? (first errs) "*-*-* 02:00:00")
          "the message should show the OnCalendar spelling, not just refuse"))
    (is (empty? (errors-matching (fixture :walg-full-backup-oncalendar "*-*-* 02:00:00")
                                 "crontab")))
    (is (empty? (errors-matching (fixture :walg-full-backup-oncalendar "daily")
                                 "crontab")))))

(deftest the-sender-must-sit-under-the-resend-sending-domain
  (testing "ONCE derives the sending domain as notifications.<zone>; a From
            address on the bare zone is not what gets verified, and Resend
            rejects mail from it"
    (is (seq (errors-matching (fixture :airflow-smtp-from "airflow@fixture.example")
                              "notifications.")))
    (is (empty? (errors-matching (fixture :airflow-smtp-from
                                          "airflow@notifications.fixture.example")
                                 "notifications.")))
    (testing "and only under Resend, since another relay verifies nothing"
      (is (empty? (errors-matching (fixture :provider-smtp "no-infra"
                                            :airflow-smtp-from "airflow@fixture.example")
                                   "notifications."))))))

(deftest emails-must-be-addresses
  (is (seq (errors-matching (fixture :alerts-email "ops") "alert address")))
  (is (seq (errors-matching (fixture :airflow-smtp-from "airflow") "sender address"))))

(deftest a-leftover-placeholder-in-a-gated-key-is-refused
  (testing "a colors.yml carries REPLACE_ME for every provider it is not using,
            and those are harmless — the template that would read them never
            renders. These are the ones where the gate fires and the placeholder
            reaches the generated file verbatim."
    (is (seq (errors-matching (fixture :caddy-acme-email "REPLACE_ME") "REPLACE_ME")))
    (is (= [] (validate/state-errors (fixture)))
        "and an absent optional key is still absent")))

;; ---------------------------------------------------------------------------
;; credentials and the environment

(deftest the-profile-parameter-is-refused-outright
  (testing "read-pars has already overwritten the file's value by the time any
            step runs, so there is nothing left to compare against — and for
            three of the four stages the profile is the ONLY thing separating
            this project's state from once-colors'"
    (is (= "COLORS_PAR_PROFILE" validate/profile-par))
    (is (seq (validate/env-errors {"COLORS_PAR_PROFILE" "someone-elses"})))
    (is (nil? (validate/env-errors {})))
    (is (nil? (validate/env-errors {"COLORS_PAR_PROFILE" ""}))
        "an empty value is not an override")))

(deftest the-machine-credentials-are-required
  (testing "this is the way this package departs from walter, which puts no
            secret on a server at all"
    (let [errs (str/join "\n" (validate/secret-errors (fixture)))]
      (doseq [par ["COLORS_PAR_GITHUB_TOKEN"
                   "COLORS_PAR_POSTGRES_PASSWORD"
                   "COLORS_PAR_AIRFLOW_FERNET_KEY"
                   "COLORS_PAR_AIRFLOW_ADMIN_PASSWORD"
                   "COLORS_PAR_WALG_R2_ACCESS_KEY_ID"
                   "COLORS_PAR_WALG_R2_SECRET_ACCESS_KEY"]]
        (is (str/includes? errs par) (str par " must be demanded"))))))

(deftest the-selected-providers-credentials-are-required
  (let [errs (str/join "\n" (validate/secret-errors (fixture :provider-backend "r2")))]
    (is (str/includes? errs "COLORS_PAR_DO_TOKEN"))
    (is (str/includes? errs "COLORS_PAR_CLOUDFLARE_API_TOKEN"))
    (is (str/includes? errs "COLORS_PAR_RESEND_API_KEY"))
    (is (str/includes? errs "COLORS_PAR_RESEND_PASSWORD"))
    (is (str/includes? errs "COLORS_PAR_R2_ACCESS_KEY_ID")))
  (testing "and an unselected provider's are not"
    (is (not (str/includes? (str/join (validate/secret-errors (fixture)))
                            "COLORS_PAR_HCLOUD_TOKEN")))))

(deftest a-supplied-credential-is-not-demanded
  (is (empty? (filter #(str/includes? % "COLORS_PAR_DO_TOKEN")
                      (validate/secret-errors (fixture :do-token "supplied"))))))

(deftest tofu-env-maps-a-credential-to-the-variable-tofu-reads
  (testing "one registry drives both validation and the process environment, so
            a provider cannot be checked against one set of keys and run with
            another"
    (is (= {:do-token "DIGITALOCEAN_TOKEN"}
           (validate/tofu-env (fixture) :provider-compute)))
    (is (= {:r2-access-key-id "AWS_ACCESS_KEY_ID"
            :r2-secret-access-key "AWS_SECRET_ACCESS_KEY"}
           (validate/tofu-env (fixture :provider-backend "r2") :provider-backend)))))

(deftest nothing-is-stoppable-in-v1
  (testing "a statement rather than an oversight: an Airflow scheduler runs
            continuously, so a box that cannot be parked costs nothing that was
            not already being paid"
    (is (= #{} validate/stoppable))
    (is (not (validate/stoppable? (fixture))))))
