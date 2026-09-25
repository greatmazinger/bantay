#!/bin/sh
# Start a program unprivileged inside a delegated cgroup v2 subtree.
#
#   sudo scripts/run-delegated.sh ./bin/server --flags
#
# This is the systemd-free equivalent of a unit with User= and Delegate=yes. It
# must run as root, but only to establish the delegation: the program it execs
# runs as an ordinary user and needs no privilege of its own.
#
# The placement is not optional. A process started outside the subtree cannot
# move itself in, because migrating between cgroups requires write access to
# cgroup.procs of the common ancestor of source and destination, and for a
# process started elsewhere that ancestor is the root cgroup. So the sequence
# has to be: place, then drop, then exec.

set -eu

CGROUP_ROOT="${CGROUP_ROOT:-/sys/fs/cgroup}"
SUBTREE="${SUBTREE:-jobworker}"
RUN_AS="${RUN_AS:-${SUDO_USER:-}}"

if [ "$(id -u)" -ne 0 ]; then
	echo "must run as root: sudo $0 $*" >&2
	exit 1
fi
if [ $# -lt 1 ]; then
	echo "usage: sudo $0 <program> [args...]" >&2
	exit 1
fi
if [ -z "$RUN_AS" ]; then
	echo "set RUN_AS to the user the program should run as" >&2
	exit 1
fi

uid=$(id -u "$RUN_AS")
gid=$(id -g "$RUN_AS")
root="$CGROUP_ROOT/$SUBTREE"
leaf="$root/server"

# The subtree, and a leaf for the server's own process. A cgroup cannot both
# hold processes and enable controllers for its children, so the server needs a
# leaf of its own rather than living in the subtree root.
mkdir -p "$leaf"

# Enabling a controller requires write access to the parent's
# cgroup.subtree_control, which the delegated user will not have. Root does it
# here, once.
echo "+cpu +memory +io +pids" > "$root/cgroup.subtree_control"

# The delegation set. cgroup.procs on the subtree root is the one that is easy
# to miss: it is what the common-ancestor check reads when the server later
# creates job cgroups and clones into them.
chown "$uid:$gid" \
	"$root" \
	"$root/cgroup.procs" \
	"$root/cgroup.subtree_control" \
	"$root/cgroup.threads" \
	"$leaf" \
	"$leaf/cgroup.procs"

# Place this shell, then become the target user, then exec. The exec preserves
# the PID, so cgroup membership carries through to the program.
echo $$ > "$leaf/cgroup.procs"
exec setpriv --reuid="$uid" --regid="$gid" --clear-groups "$@"
