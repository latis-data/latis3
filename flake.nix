{
  description = "LaTiS development environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
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
              pkgs.netcdf
              sbt
            ];

            # The NetCDF Java library needs to know the location of
            # the NetCDF C library.
            LD_LIBRARY_PATH = "${pkgs.netcdf}/lib";
          };
        }
    );
}
