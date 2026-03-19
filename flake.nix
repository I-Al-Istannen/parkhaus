{
  description = "A suite for testing compiler submissions";

  inputs = {
    naersk.url = "github:nix-community/naersk";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
    gitignore.url = "github:hercules-ci/gitignore.nix";
    gitignore.inputs.nixpkgs.follows = "nixpkgs";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      gitignore,
      nixpkgs,
      naersk,
      treefmt-nix,
    }:
    let
      forAllSystems = nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed;
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          naersk' = pkgs.callPackage naersk { };
          inherit (gitignore.lib) gitignoreFilterWith;

          build-naersk = naersk'.buildPackage {
            version = (pkgs.lib.importTOML ./Cargo.toml).package.version;
            src = pkgs.lib.cleanSourceWith {
              filter = gitignoreFilterWith {
                basePath = ./.;
                extraRules = ''
                  /.github/
                  /assets
                  /README.md
                '';
              };
              src = ./.;
            };
          };
        in
        rec {
          default = parkhaus;
          parkhaus =
            pkgs.runCommand "parkhaus" { } ''
              mkdir -p $out/bin
              cp ${build-naersk}/bin/parkhaus $out/bin
            ''
            // {
              meta.mainProgram = "parkhaus";
            };
          docker = pkgs.dockerTools.buildLayeredImage {
            name = "parkhaus";
            tag = build-naersk.version;

            contents = [
              pkgs.cacert
              pkgs.sqlite
              pkgs.busybox
            ];

            # https://discourse.nixos.org/t/dockertools-buildimage-and-user-writable-tmp/5397/9
            extraCommands = "mkdir -m 0777 tmp";

            fakeRootCommands = ''
              ${pkgs.dockerTools.shadowSetup}
            '';
            enableFakechroot = true;

            config = {
              Entrypoint = [ "${pkgs.lib.getExe parkhaus}" ];
              # The config uses the local timezone
              Env = [ "TZDIR=${pkgs.tzdata}/share/zoneinfo" ];

              Expose = {
                "8080/tcp" = { };
              };
            };
          };
        }
      );

      formatter = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          treefmtEval = treefmt-nix.lib.evalModule pkgs {
            projectRootFile = "flake.nix";
            programs.nixpkgs-fmt.enable = true;
          };
        in
        treefmtEval.config.build.wrapper
      );
    };
}
