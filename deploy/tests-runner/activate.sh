#!/usr/bin/env bash
set -euo pipefail
server=${1:?Usage: activate.sh SERVER SSH_ADMIN_KEY APP_ID INSTALLATION_ID APP_PRIVATE_KEY}
admin_key=${2:?Missing SSH administrative key path}
app_id=${3:?Missing GitHub App ID}
installation_id=${4:?Missing installation ID}
app_key=${5:?Missing App private key path}
[[ $app_id =~ ^[0-9]+$ && $installation_id =~ ^[0-9]+$ ]]
root=$(cd "$(dirname "$0")" && pwd)
ssh "$server" 'set -euo pipefail
install -d -m 700 /etc/origo-tests-runner
install -d -m 755 /opt/origo-tests-runner
if systemctl is-active --quiet origo-tests-runner; then
    echo "Stop the runner controller before rebuilding its clean image" >&2
    exit 1
fi
if ! test -f /etc/origo-tests-runner/guest-key; then
    ssh-keygen -q -t ed25519 -N "" -f /etc/origo-tests-runner/guest-key
fi'
scp "$root/controller.sh" "$root/registration.py" "$root/controller.service" \
    "$server:/opt/origo-tests-runner/"
scp "$app_key" "$server:/etc/origo-tests-runner/app.pem"
printf 'APP_ID=%s\nINSTALLATION_ID=%s\n' "$app_id" "$installation_id" | \
    ssh "$server" 'cat > /etc/origo-tests-runner/app.env; chmod 600 /etc/origo-tests-runner/app.env /etc/origo-tests-runner/app.pem'
address=$(ssh "$server" 'virsh domifaddr origo-tests | awk '\''/ipv4/ {sub("/.*", "", $4); print $4}'\''')
test -n "$address"
guest_ssh=(-o ProxyJump="$server" -o BatchMode=yes -i "$admin_key")
ssh "$server" 'cat /etc/origo-tests-runner/guest-key.pub' | \
    ssh "${guest_ssh[@]}" "runner@$address" 'cat >> ~/.ssh/authorized_keys'
scp -o ProxyJump="$server" -i "$admin_key" "$root/admit-job.sh" "runner@$address:/tmp/origo-admit-job.sh"
ssh "${guest_ssh[@]}" "runner@$address" 'set -euo pipefail
if pgrep -f '\''[p]ython.*-m pytest'\''; then echo "Runtime profiling is still active" >&2; exit 1; fi
sudo install -o root -g root -m 755 /tmp/origo-admit-job.sh /opt/origo-ci/admit-job.sh
printf "ACTIONS_RUNNER_HOOK_JOB_STARTED=/opt/origo-ci/admit-job.sh\n" > /opt/actions-runner/.env
rm -f /opt/actions-runner/.runner /opt/actions-runner/.credentials /opt/actions-runner/.credentials_rsaparams'
ssh "$server" bash -s -- "$address" <<'REMOTE'
set -euo pipefail
address=$1
ssh-keyscan -t ed25519 "$address" 2>/dev/null | sed "s/^$address/origo-tests/" > /etc/origo-tests-runner/known_hosts
chmod 600 /etc/origo-tests-runner/known_hosts
virsh autostart origo-tests --disable
virsh shutdown origo-tests
for attempt in {1..60}; do
    if [[ $(virsh domstate origo-tests) == 'shut off' ]]; then break; fi
    sleep 1
done
[[ $(virsh domstate origo-tests) == 'shut off' ]]
cd /var/lib/libvirt/images/origo-tests
qemu-img convert -r 104857600 -O qcow2 runner.qcow2 clean.qcow2
chmod 444 clean.qcow2
install -m 644 /opt/origo-tests-runner/controller.service /etc/systemd/system/origo-tests-runner.service
systemctl daemon-reload
systemctl enable --now origo-tests-runner
REMOTE
