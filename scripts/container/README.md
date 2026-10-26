# Linux container with io_uring on macOS

`linux.sh` runs a cargo command in a Linux container on a dedicated Podman
machine, `straum-test`, so that the storage engine's tests run on its
io_uring driver from a macOS workstation. Container results are functional
checks only, never performance evidence.

```bash
scripts/container/linux.sh --switch-machine test --workspace
```

The test `linux_runtime_uses_io_uring` confirms that the io_uring driver was
used.

## Why a dedicated machine

Rootless Podman blocks io_uring twice:

1. **seccomp.** The default profile (`/usr/share/containers/seccomp.json`
   from containers-common 0.68.1) has no rule for the io_uring syscalls, so
   `io_uring_setup` returns ENOSYS. `seccomp-io-uring.json` is that profile
   with one rule appended: `SCMP_ACT_ALLOW` for `io_uring_setup`,
   `io_uring_enter` and `io_uring_register`. Every other rule is unchanged.
2. **SELinux.** With the profile above, `io_uring_setup` returns EACCES:
   the Fedora CoreOS VM (Enforcing, container-selinux 2.250.0) denies
   `container_t` the `create` permission on `anon_inode [io_uring]`. No
   SELinux boolean covers it. `straum_store_io_uring.cil` is a one-rule
   module: `allow container_t io_uring_t:anon_inode { create map read write }`.

Both relaxations stay inside `straum-test`. Seccomp is not disabled and
containers do not run `--privileged`.

## Setup

```bash
podman machine init straum-test --cpus 4 --memory 2048 --disk-size 60
podman machine stop podman-machine-default     # one machine runs at a time
podman machine start straum-test

podman machine ssh straum-test 'cat > /tmp/m.cil' < scripts/container/straum_store_io_uring.cil
podman machine ssh straum-test 'sudo semodule -i /tmp/m.cil'
podman machine ssh straum-test 'cat > /tmp/s.json' < scripts/container/seccomp-io-uring.json
podman machine ssh straum-test 'sudo install -D -m 0644 /tmp/s.json /var/lib/straum-store/seccomp-io-uring.json'
podman machine ssh straum-test 'mkdir -p ~/.config/containers/containers.conf.d && printf "[containers]\nseccomp_profile = \"/var/lib/straum-store/seccomp-io-uring.json\"\n" > ~/.config/containers/containers.conf.d/50-straum-store-seccomp.conf'
```

Install the profile under `/var` inside the VM, not under `/etc/containers`:
on Podman machines for macOS, `/etc/containers` is a virtiofs mount of the
host's `~/.config/containers`, shared by every machine and by the macOS
podman client.

Remove the module with `podman machine ssh straum-test 'sudo semodule -r
straum_store_io_uring'`.

## How the script treats running machines

Podman on applehv runs one machine at a time. The script never stops a
machine on its own:

- another machine is running: it changes nothing, names that machine and
  exits with status 2;
- `--switch-machine` as the first argument: it stops the running machine,
  starts `straum-test`, runs the command, then stops `straum-test` and
  starts the previous machine again, also when the command fails or the
  script gets SIGINT or SIGTERM; the exit status is the command's;
- nothing is running: `straum-test` is started for the command and stopped
  after it;
- `--session` as the first argument: as `--switch-machine`, but
  `straum-test` stays running afterwards, so later commands start at once;
  the previous machine stays stopped until `--end-session`, which stops
  `straum-test` and starts Podman's default machine. A running
  `straum-test` is always reused. `scripts/validate.sh push` and `pr` use
  this mode.

The container image is pinned by digest. Cargo's registry and target
directory live in named volumes (`straum-container-registry`,
`straum-container-target`), so macOS and Linux build artifacts never mix.
