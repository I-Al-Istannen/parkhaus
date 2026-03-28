{
  description = "A suite for testing compiler submissions";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
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
      crane,
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
          craneLib = crane.mkLib pkgs;
          inherit (gitignore.lib) gitignoreFilterWith;
          pname = "parkhaus";

          build-crane =
            craneLib.buildPackage {
              inherit pname;
              version = (pkgs.lib.importTOML ./Cargo.toml).workspace.package.version;
              doCheck = false;

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
            }
            // {
              meta.mainProgram = pname;
            };
        in
        rec {
          default = parkhaus;
          parkhaus =
            pkgs.runCommand pname { } ''
              mkdir -p $out/bin
              cp ${pkgs.lib.getExe build-crane} $out/bin
            ''
            // {
              meta.mainProgram = pname;
            };
          docker = pkgs.dockerTools.buildLayeredImage {
            name = pname;
            tag = build-crane.version;

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

              ExposedPorts = {
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
