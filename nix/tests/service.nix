{ pkgs, module }:
let
  serviceProbe = pkgs.writeShellApplication {
    name = "auxide-service-probe";
    runtimeInputs = [ pkgs.coreutils ];
    text = ''
      [[ "$#" == 3 && "$1" == --config && "$3" == run ]]
      [[ "$(cat "$2")" == fixture-config ]]
      [[ "$(cat "$CREDENTIALS_DIRECTORY/discord-token")" == fixture-token ]]
      [[ "$EUID" != 0 ]]
      [[ -w "$RUNTIME_DIRECTORY" && -w "$CACHE_DIRECTORY" ]]
      [[ ! -r /var/lib/auxide/config.toml ]]
      [[ ! -w /etc/passwd ]]
      printf '%s\n' "$EUID" > "$RUNTIME_DIRECTORY/ready"
      trap 'touch "$CACHE_DIRECTORY/stopped"; exit 0' TERM
      while true; do sleep 1 & wait "$!"; done
    '';
  };
in
pkgs.testers.runNixOSTest {
  name = "auxide-service";

  nodes.machine = {
    imports = [ module ];
    services.auxide = {
      enable = true;
      package = serviceProbe;
      poTokenProvider.enable = false;
    };
    virtualisation.memorySize = 768;
  };

  testScript = ''
    import shlex

    machine.start()
    machine.wait_for_unit("multi-user.target")

    with subtest("missing credentials prevent startup"):
        machine.wait_until_succeeds(
            "test $(systemctl show auxide.service -p ExecMainStatus --value) = 243"
        )
        machine.fail("test -e /run/auxide/ready")
        machine.fail("auxide-credential status")
        machine.succeed("systemctl stop auxide.service")

    with subtest("host-encrypted credentials reach the unprivileged service"):
        machine.succeed("printf fixture-config > /var/lib/auxide/config.toml")
        machine.succeed("chmod 600 /var/lib/auxide/config.toml")
        machine.succeed(
            "printf fixture-token | systemd-creds encrypt --with-key=host "
            "--name=discord-token - /var/lib/auxide/discord-token"
        )
        machine.succeed("chmod 600 /var/lib/auxide/discord-token")
        machine.succeed("auxide-credential status")
        machine.fail("auxide-credential set")
        machine.succeed("systemctl reset-failed auxide.service; systemctl start auxide.service")
        machine.wait_for_unit("auxide.service")
        machine.wait_for_file("/run/auxide/ready")
        uid = machine.succeed("id -u auxide").strip()
        assert machine.succeed("cat /run/auxide/ready").strip() == uid
        for path in ["/run/auxide", "/var/cache/auxide"]:
            assert machine.succeed(f"stat -c '%U:%G %a' {path}").strip() == "auxide:auxide 700"
        assert machine.succeed("stat -c '%U:%G %a' /var/lib/auxide").strip() == "root:root 700"
        machine.fail("su -s /bin/sh nobody -c 'cat /var/lib/auxide/discord-token'")
        machine.fail("su -s /bin/sh nobody -c 'auxide-credential status'")

    with subtest("a crashed process is restarted"):
        old_pid = machine.succeed("systemctl show auxide.service -p MainPID --value").strip()
        machine.succeed("rm /run/auxide/ready")
        machine.succeed(f"kill -KILL {old_pid}")
        machine.wait_until_succeeds(
            "pid=$(systemctl show auxide.service -p MainPID --value); "
            f"test \"$pid\" != 0 && test \"$pid\" != {shlex.quote(old_pid)}"
        )
        machine.wait_for_file("/run/auxide/ready")
        assert int(machine.succeed("systemctl show auxide.service -p NRestarts --value")) >= 1

    with subtest("stopping the service cleans runtime data and retains cache data"):
        machine.succeed("systemctl stop auxide.service")
        machine.succeed("test -f /var/cache/auxide/stopped")
        machine.fail("test -e /run/auxide")
        assert machine.succeed("systemctl show auxide.service -p ActiveState --value").strip() == "inactive"

    with subtest("corrupt credentials prevent startup and can be replaced"):
        machine.succeed("cp /var/lib/auxide/discord-token /var/lib/auxide/token-backup")
        machine.succeed("printf invalid > /var/lib/auxide/discord-token")
        machine.fail("auxide-credential status")
        machine.succeed("systemctl start auxide.service")
        machine.wait_until_succeeds(
            "test $(systemctl show auxide.service -p ExecMainStatus --value) = 243"
        )
        machine.fail("test -e /run/auxide/ready")
        machine.succeed("systemctl stop auxide.service")
        machine.succeed("mv /var/lib/auxide/token-backup /var/lib/auxide/discord-token")
        machine.succeed("systemctl reset-failed auxide.service; systemctl start auxide.service")
        machine.wait_for_file("/run/auxide/ready")
        machine.succeed("systemctl stop auxide.service")
        logs = machine.succeed("journalctl -u auxide.service --no-pager")
        assert "fixture-token" not in logs
  '';
}
