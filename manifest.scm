(use-modules
 (gnu packages bash)
 (gnu packages java)
 (gnu packages maths)
 (gnu packages ncurses)
 (guix build-system copy)
 (guix download)
 ((guix licenses) #:prefix license:)
 (guix packages))

(define coursier
  (package
   (name "coursier")
   (version "2.1.24")
   (source (origin
            (method url-fetch)
            (uri
             (string-append
              "https://github.com/coursier/coursier/releases/download/v"
              version
              "/coursier.jar"))
            (sha256
             (base32
              "0c7k2ibww0sgn0smyfb2mgvmi5lp4kv0mfk3hbm56hsk0k14swlc"))))
   (build-system copy-build-system)
   (arguments
    (list
     #:install-plan ''(("coursier.jar" "share/java/coursier.jar"))
     #:phases
     #~(modify-phases
        %standard-phases
        ;; Make an executable wrapper that runs the JAR file.
        (add-after 'install 'wrap-program
                   (lambda* (#:key inputs outputs #:allow-other-keys)
                     (let* ((out (assoc-ref outputs "out"))
                            (bin (string-append out "/bin"))
                            (jar (string-append out "/share/java/coursier.jar"))
                            (wrapper (string-append bin "/cs"))
                            (sh (string-append (assoc-ref inputs "bash")
                                               "/bin/sh"))
                            (java (string-append (assoc-ref inputs "openjdk")
                                                 "/bin/java")))
                       (mkdir-p bin)
                       (with-output-to-file wrapper
                         (lambda ()
                           (display
                            (string-append
                             "#!" sh "\n\n"
                             java " -jar " jar " \"$@\"\n"))))
                       (chmod wrapper #o755)))))))
   (propagated-inputs (list `(,openjdk21 "jdk")
                            ;; Uses tput.
                            ncurses))
   (synopsis "Pure Scala Artifact Fetching")
   (description "Coursier is the Scala application and artifact manager.
It can install Scala applications and setup your Scala development
environment. It can also download and cache artifacts from the web.")
   (home-page "https://get-coursier.io")
   (license license:asl2.0)))

(define* (coursier-fetch #:key coursier)
  (lambda* (src hash-algo hash #:optional name #:key (system (%current-system)))
    "Return a fixed-output derivation that fetches the dependencies of
SRC (expected to be a Maven coordinate) using Coursier (\"cs fetch\").
The output is expected to have recursive hash HASH of type
HASH-ALGO (a symbol). Use NAME as the file name, or \"coursier-fetch\"
if #f."
    (gexp->derivation
     (or name "coursier-fetch")
     (with-imported-modules
      '((guix build utils)
        (ice-9 popen)
        (ice-9 ports)
        (ice-9 rdelim))
      #~(begin
          (use-modules (guix build utils)
                       (ice-9 popen)
                       (ice-9 ports)
                       (ice-9 rdelim))
          (setenv "COURSIER_CACHE" (getcwd))
          (let* ((jar-dir (string-append #$output "/share/java"))
                 (cs (string-append #$coursier "/bin/cs"))
                 (cmd (string-append cs " fetch " #$src))
                 (port (open-input-pipe cmd)))
            ;; Install the JAR files downloaded by coursier.
            (call-with-port port
              (lambda (port)
                (for-delimited-from-port
                 port
                 (lambda (file)
                   (install-file file jar-dir))))))))
     #:hash hash
     #:hash-algo hash-algo
     #:recursive? #t
     #:system system)))

(define metals
  (package
   (name "metals")
   (version "1.6.7")
   (source (origin
            (method (coursier-fetch #:coursier coursier))
            (uri (string-append "org.scalameta:metals_2.13:" version))
            (sha256
             (base32
              "1pbn8mrmjl0z1gkwwckjb77d6kd7w175xzzv8fg3nsd004ypfv3c"))))
   (build-system copy-build-system)
   (arguments
    (list
     #:install-plan ''(("share/java" "share/java"))
     #:phases
     #~(modify-phases
        %standard-phases
        ;; Make an executable wrapper.
        (add-after 'install 'make-wrapper
                   (lambda* (#:key inputs outputs #:allow-other-keys)
                     (let* ((out (assoc-ref outputs "out"))
                            (classpath (string-append out "/share/java/*"))
                            (bin-dir (string-append out "/bin"))
                            (wrapper (string-append bin-dir "/metals"))
                            (sh (string-append (assoc-ref inputs "bash")
                                               "/bin/sh"))
                            (java (string-append (assoc-ref inputs "openjdk")
                                                 "/bin/java")))
                       (mkdir-p bin-dir)
                       (with-output-to-file wrapper
                         (lambda ()
                           (display
                            (string-append
                             "#!" sh "\n"
                             java " -cp \"" classpath "\" scala.meta.metals.Main"))))
                       (chmod wrapper #o755)))))))
   (propagated-inputs (list bash `(,openjdk21 "jdk")))
   (synopsis "Scala language server")
   (description "Metals is a Scala language server with rich IDE features.")
   (home-page "https://scalameta.org/metals")
   (license license:asl2.0)))

(define netcdf-with-ld-path
  (package
   (inherit netcdf)
   (native-search-paths
    (list (search-path-specification
           (variable "LD_LIBRARY_PATH")
           (files (list "lib/")))))))

(define sbt
  (package
   (name "sbt")
   (version "1.12.11")
   (source (origin
            (method url-fetch)
            (uri (string-append "https://github.com/sbt/sbt/releases/download/v"
                                version
                                "/sbt-"
                                version
                                ".tgz"))
            (sha256
             (base32
              "0ia62adwz51gi6ppgikfdc58dc3bd1gc6x0s2hlqzgx5s9wjm5sz"))))
   (build-system copy-build-system)
   (arguments
    ;; NOTE: Not installing sbtn.
    (list #:install-plan ''(("bin/sbt" "bin/sbt"))))
   (propagated-inputs (list
                       `(,openjdk21 "jdk")
                       ;; For infocmp
                       ncurses))
   (synopsis "Build tool for Scala")
   (description "SBT, a simple build tool for Scala.")
   (home-page "https://www.scala-sbt.org")
   (license license:asl2.0)))

(packages->manifest
 (list metals
       netcdf-with-ld-path
       sbt))
