#!/bin/bash

set -e

SOURCE=""
DEST=""
REMOTE_USER=""
REMOTE_HOST=""

trap cleanup_function INT

#################### FUNCTIONS########################################
usage() {
  echo "$*" 1>&2
  echo "Usage: $0 [-u remote_user] [-h remote_host] [-d destination] source" 1>&2
}

die_usage() {
  usage $*
  exit 1
}

cleanup_function() {
  set -x
  rm -f /tmp/$$.*
  [ "$SSH_AGENT_PID" != "" ] && kill $SSH_AGENT_PID
}

#################### MAIN PROGRAM ####################################
# Parse command-line options
while getopts ":h:u:d:" opt ; do
  case "$opt" in

    d)
      if [ "$DEST" != "" ] ; then usage "Cannot have 2 destinations" 1>&2 ; exit 1; fi
      DEST="$OPTARG"
      echo "Destination: $DEST" 1>&2
      ;;

    u)
      REMOTE_USER="$OPTARG"
      ;;

    h)
      REMOTE_HOST="$OPTARG"
      ;;

    *)
      usage "Invalid option: -$OPTARG" 1>&2; exit 1
      ;;
  esac
done

# Source directory (mass arguments)
shift $((OPTIND-1))

if [ "$#" -ne 1 ]
then
  usage "Only one source directory accepted" 1>&2 ; exit 1
fi
SOURCE=$1
SOURCE=$( echo "$SOURCE" | sed 's#/*$##' ) # Remove trailing slashes
echo "Source: $SOURCE" 1>&2

[ "$REMOTE_USER" = "" ] && REMOTE_USER=$(whoami)

# Source: local vs remote
case "$REMOTE_HOST" in

'')
  # Local dir
  RSYNC_OPTS=""
  RSYNC_SOURCE="$SOURCE"
  find "$SOURCE" -type f | sed "s#^${SOURCE}/*##"> /tmp/$$.all
  ;;

*)
  # Remote host
  echo "Remote account: ${REMOTE_USER}@${REMOTE_HOST}"
  echo "$SOURCE" | grep "^/" || die_usage "Only absolute paths accepted in source dir"
  ping -c1 "$REMOTE_HOST"
  RSYNC_OPTS="-z" # useful for network
  ssh-agent | grep -v echo > /tmp/$$.ssh
  . /tmp/$$.ssh
  ssh-add
  RSYNC_SOURCE="${REMOTE_USER}@${REMOTE_HOST}:${SOURCE}"
  ssh -l "$REMOTE_USER" $REMOTE_HOST "find $SOURCE -type f" | sed "s#^${SOURCE}/*##"> /tmp/$$.all
  ;;

esac

# Get NUMA nodes
NODES=$( numactl --hardware | grep '^node [0-9].*cpus' | awk '{print $2}' | sort -u )
N_NODES=$( numactl --hardware | grep '^node [0-9].*cpus' | awk '{print $2}' | sort -u | wc -l )
echo "$N_NODES nodes found" 1>&2

set -x
# Split files among processes
for node in $NODES
do
  echo "Node: $node"
  cat /tmp/$$.all | awk "(NR%${N_NODES}==${node}%${N_NODES})" > /tmp/$$.${node}
done

# Create destination directory
if [ "$DEST" = "" ]
then
  echo "No destination specified, will copy to /dev/shm" 1>&2
  DEST=/dev/shm
fi

case "$DEST" in
  /dev/shm*)
    sudo mkdir -p "$DEST"
  ;;
  *)
    echo "Create tmpfs $DEST ..." 1>&2
    DEST=$( echo "$DEST" | sed 's#/*$##' ) # Remove trailing slashes
    sudo mkdir -p "$DEST"
    df "$DEST" | grep "$DEST" > /dev/null && sudo umount "$DEST"
    #WAS: sudo mount -t tmpfs -o noatime,size=40g,mpol=bind:0 tmpfs .....
    sudo mount -t tmpfs -o noatime tmpfs "$DEST"
    #DEBUG find "$DEST" -type f
  ;;
esac 

# Copy processes -- one per node
for node in  $NODES ; do

  numactl --membind=${node} --cpunodebind=${node} \
    rsync -a $RSYNC_OPTS --files-from=/tmp/$$.${node} ${RSYNC_SOURCE} ${DEST}/ &

done

wait $(jobs -p)
echo "Done waiting for cpoy processes." 1>&2

#DEBUG ssh -l "$REMOTE_USER" $REMOTE_HOST 'whoami ; hostname -f'


cleanup_function

