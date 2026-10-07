#!/usr/bin/env bash
set -euo pipefail

server=${1:?Usage: provision_tests_runner.sh SERVER SSH_PUBLIC_KEY}
public_key=${2:?Usage: provision_tests_runner.sh SERVER SSH_PUBLIC_KEY}
repo_root=$(cd "$(dirname "$0")/.." && pwd)
ssh-keygen -l -f "$public_key" >/dev/null

ssh "$server" 'mkdir -p /var/lib/libvirt/images/origo-tests'
scp "$repo_root/deploy/tests-runner/cloud-init.yaml" \
    "$repo_root/deploy/tests-runner/isolation.xml" \
    "$server:/var/lib/libvirt/images/origo-tests/"
scp "$public_key" "$server:/var/lib/libvirt/images/origo-tests/admin.pub"

ssh "$server" bash -s <<'REMOTE'
set -euo pipefail
cd /var/lib/libvirt/images/origo-tests
limits() {
    flags=(--config)
    if [[ $(virsh domstate origo-tests) == running ]]; then flags+=(--live); fi
    virsh schedinfo origo-tests --set global_period=100000 --set global_quota=1200000 "${flags[@]}"
    virsh memtune origo-tests --hard-limit 29360128 --swap-hard-limit 29360128 "${flags[@]}"
    virsh blkdeviotune origo-tests vda --total-bytes-sec 104857600 --total-iops-sec 2000 "${flags[@]}"
}
if virsh dominfo origo-tests >/dev/null 2>&1; then
    limits
    virsh domifaddr origo-tests
    exit 0
fi
test -c /dev/kvm
python3 - <<'PY'
from pathlib import Path
template = Path('cloud-init.yaml')
template.write_text(template.read_text().replace('__SSH_PUBLIC_KEY__', Path('admin.pub').read_text().strip()))
PY
curl --fail --location --silent --show-error \
    https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img -o base.qcow2
curl --fail --location --silent --show-error \
    https://cloud-images.ubuntu.com/noble/current/SHA256SUMS -o SHA256SUMS
grep 'noble-server-cloudimg-amd64.img$' SHA256SUMS \
    | sed 's/noble-server-cloudimg-amd64.img/base.qcow2/' | sha256sum -c -
qemu-img create -f qcow2 -F qcow2 -b "$PWD/base.qcow2" "$PWD/runner.qcow2" 100G
virsh nwfilter-define isolation.xml
virt-install --name origo-tests --memory 24576 --vcpus 12 --cpu host-passthrough \
    --import --disk "$PWD/runner.qcow2,format=qcow2,bus=virtio" \
    --network network=default,model=virtio,filterref=origo-tests-isolation \
    --os-variant ubuntu22.04 --cloud-init "user-data=$PWD/cloud-init.yaml" \
    --graphics none --noautoconsole
limits
REMOTE
