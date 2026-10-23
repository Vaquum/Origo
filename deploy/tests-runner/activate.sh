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
install -d -m 700 /etc/origo-test-runner
install -d -m 755 /opt/origo-test-runner
if systemctl is-active --quiet origo-test-runner; then
    echo "Stop the runner controller before rebuilding its clean image" >&2
    exit 1
fi
if ! test -f /etc/origo-test-runner/guest-key; then
    ssh-keygen -q -t ed25519 -N "" -f /etc/origo-test-runner/guest-key
fi'
scp "$root/controller.sh" "$root/registration.py" "$root/controller.service" \
    "$server:/opt/origo-test-runner/"
scp "$app_key" "$server:/etc/origo-test-runner/app.pem"
printf 'APP_ID=%s\nINSTALLATION_ID=%s\n' "$app_id" "$installation_id" | \
    ssh "$server" 'cat > /etc/origo-test-runner/app.env; chmod 600 /etc/origo-test-runner/app.env /etc/origo-test-runner/app.pem'
address=$(ssh "$server" 'virsh domifaddr origo-test-runner | awk '\''/ipv4/ {sub("/.*", "", $4); print $4}'\''')
test -n "$address"
guest_ssh=(-o ProxyJump="$server" -o BatchMode=yes -i "$admin_key")
ssh "$server" 'cat /etc/origo-test-runner/guest-key.pub' | \
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
ssh-keyscan -t ed25519 "$address" 2>/dev/null | sed "s/^$address/origo-test-runner/" > /etc/origo-test-runner/known_hosts
chmod 600 /etc/origo-test-runner/known_hosts
virsh autostart origo-test-runner --disable
virsh shutdown origo-test-runner
for attempt in {1..60}; do
    if [[ $(virsh domstate origo-test-runner) == 'shut off' ]]; then break; fi
    sleep 1
done
[[ $(virsh domstate origo-test-runner) == 'shut off' ]]
cd /var/lib/libvirt/images/origo-test-runner
qemu-img convert -r 104857600 -O qcow2 runner.qcow2 clean.new.qcow2
chmod 444 clean.new.qcow2
mv -f clean.new.qcow2 clean.qcow2
install -m 644 /opt/origo-test-runner/controller.service /etc/systemd/system/origo-test-runner.service
systemctl daemon-reload
systemctl enable --now origo-test-runner
REMOTE
