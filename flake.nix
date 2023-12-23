{
  description = "LaTiS development environment";

  inputs = {
    nixpkgs.url = github:nixos/nixpkgs/nixpkgs-unstable;
    flake-utils.url = github:numtide/flake-utils;
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        jre = pkgs.temurin-bin-17;
        metals = pkgs.metals.override { jre = jre; };
        sbt = pkgs.sbt.override { jre = jre; };
      in
        {
          devShell = pkgs.mkShell {
            packages = [
              jre
              metals
              sbt
            ];
          };
        }
    );
}
