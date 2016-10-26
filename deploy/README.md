The script multi-copy.sh performs the following steps:

"	Check if the source dir is local or remote. Set up ssh-agent for passwordless SSH access to remote host.
o	Start ssh-agent
o	Ask for SSH key passphrase once via ssh-add. From this point onwards, there is no need to enter the SSH passphrase for either SSH or rsync connections to the remote source host.
"	Get list of NUMA nodes using numactl -hardware
"	Create tmpfs for the destination directory, not bound to any NUMA node:
o	sudo mkdir -p "$DEST" # DEST is the destination directory, for example  /var/tmp/destination1/
o	sudo mount -t tmpfs -o noatime tmpfs "$DEST"
"	Get list of files in source dir (may need to use SSH for this)
"	Split the list of files in round-robin fashion and create N files, where N is the list of NUMA nodes
"	For each list of files, run 'rsync' in the background to copy the files, either locally or remotely:
o	  numactl --membind=${node} --cpunodebind=${node} rsync -a $RSYNC_OPTS --files-from=/tmp/$$.${node} ${RSYNC_SOURCE} ${DEST}/ &
"	Wait until the N rsync background processes finish executing

For example, if the source dir is /var/tmp/source_dir_1 in the remote node simpl-l4tm-580-18, and the destination dir is /var/tmp/destination1/, there will be 4 rsync processes started like this:
                                                          
numactl --membind=0 --cpunodebind=0 rsync -a -z --files-from=/tmp/40465.0 hernan@simpl-l4tm-580-18:/var/tmp/source_dir_1 /var/tmp/destination1/ &

with membind=0..4 and cpunodebind=0..4

In the end, the same directory structure that was present under /var/tmp/source_dir_1  is replicated under /var/tmp/destination1/

The script is invoked as follows:

$ ./multi-copy.sh -d /var/tmp/destination1 -h $(hostname) -u hernan /var/tmp/source_dir_1

Parameters are:
"	-d destination directory
"	-h remote host of source directory (if absent, will assume source dir is local)
"	-u remote user (if absent, will use same username as invoked the script)
"	Source directory (on either remote host or local)

Note: I am assuming that the user has set up SSH authorized_keys on the remote host, etc, so we can have passwordless SSH connections once ssh-agent is running and asks for the SSH key passphrase once.
