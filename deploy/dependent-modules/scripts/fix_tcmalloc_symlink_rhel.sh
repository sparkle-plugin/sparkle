#!/bin/bash
# Install pre-requisite software for sparkle.

# Fix tcmalloc symlink
# Find file or symlink .../libtcmalloc.so.NNN (with NNN an int)
# and symlink to libtcmalloc.so.
fix_tcmalloc ()
{
TCMALLOC="/usr/lib64/libtcmalloc.so"
if [ -e "$TCMALLOC" ]
then
	:
else
	TCLINK=$( ls ${TCMALLOC}.* | grep '\.so.[0-9]*$' )
	if [ -z "$TCLINK" ]
	then
		echo "Could not find symlink ${TCMALLOC}.NNN" 1>&@
		exit 1
	fi
	if [ -e "${TCLINK}" ]
	then
		echo "Create symbolic link from $TCMALLOC to $TCLINK" 1>&2
		sudo ln -s $( basename "$TCLINK" ) "${TCMALLOC}"
		cmp "$TCLINK" "${TCMALLOC}"
	else
		echo "Could not find symlink ${TCMALLOC}.NNN -- giving up" 1>&2
		echo "Please create the $TCMALLOC symlink before proceeding." 1>&2
		exit 1
	fi
fi
}

fix_tcmalloc
